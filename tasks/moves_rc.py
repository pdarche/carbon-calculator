#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Module for extracting Moves data """

import datetime
import dateutil.parser
import itertools
import logging
import math
import pickle
import time
import json

import numpy as np
import moves as mvs
import pymongo
import requests


KEY = '20f32c9fad4aebc9998f8ce569bdc358'
BASE = 'http://impact.brighterplanet.com/'
TYPES = {
 'car': 'automobile_trips.json',
 'subway': 'rail_trips.json?class=commuter',
 'airplane': 'flights.json',
 'bus': 'bus_trips.json'
}


def datetime_to_seconds(dt):
    """ Converts datetime to seconds """
    return (dt.hour * 3600) + (dt.minute * 60) + dt.second

# Helper functions
def distance_on_unit_sphere(lat1, long1, lat2, long2):
    """ Computes the distance between two points on a sphere """
    # Convert latitude and longitude to
    # spherical coordinates in radians.
    degrees_to_radians = math.pi/180.0

    # phi = 90 - latitude
    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians

    # theta = longitude
    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians

    # Compute spherical distance from spherical coordinates.
    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) +
           math.cos(phi1)*math.cos(phi2))
    arc = math.acos(cos)

    # Multiply arc by the radius of the earth
    earth_rad_miles = 3963.1676
    earth_rad_feet = earth_rad_miles

    return arc * earth_rad_feet


class ExtractMovesResource(object):
    """ Extracts a Moves resource for a user

    A class for backfilling missing resources for a moves
    user.

    """
    def __init__(self, user, db, collection, moves):
        self.user = user
        self.db = db
        self.collection = collection
        self.moves = moves

    def _find_existing_dates(self):
        """ Finds the earliest update for a moves record. """
        docs = self.db[self.collection].distinct('date')
        dates = [dateutil.parser.parse(doc).date() for doc in docs]
        return dates

    def _find_no_transport_dates(self):
        """ Finds the dates that don't have transport records """
        docs = self.db.no_transport_dates.distinct('date')
        dates = [dateutil.parser.parse(date).date() for date in docs]
        return dates

    def _create_no_transport_dates(self, no_transport_dates):
        """ Creates a list of no_transport_date dicts """
        return [{'date': d.strftime('%Y-%m-%d')} for d in no_transport_dates]

    def _create_active_daterange(self, start_date):
        """ Creates a list of datatime date objects from starting with
        the date the person joined Moves to yesterday (the last full day).
        """
        base_date = dateutil.parser.parse(start_date)
        yesterday = datetime.datetime.today() - datetime.timedelta(1)
        numdays = (yesterday - base_date).days
        for x in range(0, numdays):
            yield (yesterday - datetime.timedelta(days=x)).date()

    def _create_missing_dates(self, service_dates, existing_dates,
                              no_transport_dates):
        """ Returns a list of dates that haven't been fetched """
        return [date for date in service_dates
                if date not in existing_dates
                and date not in no_transport_dates]

    def _fetch_resource(self, resource, date, update_since=None):
        """ Fetches a user's Moves summary for a given date range """
        if resource not in ['summary', 'activities', 'places', 'storyline']:
            raise ValueError('Invalid Moves resource.')

        rsrc_path = "user/{}/daily/{}?".format(resource, date)

        if resource == 'storyline':
            rsrc_path = "%s&trackPoints=true" % rsrc_path

        if update_since:
            rsrc_path = "%s&updateSince>T%sZ" % (rsrc_path, update_since)

        resources = self.moves.api(rsrc_path, 'GET').json()
        return resources

    def _extract_segments(self, storylines):
        """ Extracts the segments from a list of storyline dicts """
        segments = [s.get('segments') for s in storylines
                    if s.get('segments')]
        return itertools.chain(*segments)

    def _extract_activities(self, segments):
        """ Extracts the actiities from segments """
        activities = [s.get('activities') for s in segments
                      if s.has_key('activities')]
        return itertools.chain(*activities)

    def _extract_transports(self, activities):
        """ Extracts the transportation activities from user activities """
        return filter(lambda a: a.get('activity')
                      in ('transport', 'airplane'), activities)

    def _write_transports(self, transports):
        """ Writes the new transports to a temp file """
        with open('temp.json', 'w') as tempfile:
            json.dump(transports, tempfile)

    def run(self):
        """ Executes the run """
        join_date = self.user.get('profile', {}).get('firstDate')
        active_dates = self._create_active_daterange(join_date)
        existing_dates = self._find_existing_dates()
        no_transport_dates = self._find_no_transport_dates()
        missing_dates = self._create_missing_dates(
            active_dates, existing_dates, no_transport_dates)

        transports = []
        for date in missing_dates[:LIMIT]:
            storyline = self._fetch_resource('storyline', date)
            segments = self._extract_segments(storyline)
            activities = self._extract_activities(segments)
            transport = self._extract_transports(activities)
            if transport:
                transports.append(transport)
            else:
                db.no_transport_dates.insert(
                    {'date': date.strftime('%Y-%m-%d')})

        records = list(itertools.chain(*transports))
        # write a temp file
        self._write_transports(records)


class TransformTransports(object):
    """ Predicts the transportation type of a moves transport record """

    def __init__(self, db, collection, transports, model):
        self.db = db
        self.collection = collection
        self.transports = transports
        self.model = model

    def _create_geometry(self, trackPoint):
        """ Makes a geojson geometry """
        return {
            'type': 'Point',
            'coordinates': [trackPoint['lon'], trackPoint['lat']]
        }

    def _create_property(self, trackPoint):
        """ Makes a geojson property dict """
        return {
            'latitude': trackPoint['lat'],
            'longitude': trackPoint['lon'],
            'time': trackPoint['time'],
            'id': 'transport'
        }

    def _create_feature(self, trackPoint):
        """ Makes a geojson feature dict """
        return {
            'type': 'Feature',
            'properties': self._create_property(trackPoint),
            'geometry': self._create_geometry(trackPoint)
        }

    def _create_feature_collection(transport):
        """ makes a geojson feature collection dict """
        return {
            'type': 'FeatureCollection',
            'features': [self._create_feature(tp)
                         for tp in transport.get('trackPoints')]
        }

    def _transform_transport(transport):
        """ Updates time information for a transport dict """
        transport['date'] = dateutil.parser.parse(transport['startTime']).strftime("%Y-%m-%d")
        transport['startDatetime'] = dateutil.parser.parse(transport['startTime'])
        transport['endDatetime'] = dateutil.parser.parse(transport['endTime'])
        return transport

    def _compute_min_distance(lat_lng, station_points):
        """
        Computes the smallest distance between the start a point
        and a subway entrance.
        """
        distances = [distance_on_unit_sphere(lat_lng[1], lat_lng[0], point[1], point[0])
                     for point in station_points]
        return min(distances)

    def _compute_total_distance(self, first_point, last_point, station_points):
        """
        Computes distance between the smallest starting and end point
        of a given transport.
        """
        start = self._compute_min_distance(first_point, station_points)
        end = self._compute_min_distance(last_point, station_points)

        return start + end

    def _create_features(self, transport, station_points):
        """ Creates the features for use in the model """
        start_time = dateutil.parser.parse(transport['startTime'])
        end_time = dateutil.parser.parse(transport['endTime'])
        fp = transport['trackPoints'][0]
        lp = transport['trackPoints'][-1]
        features = [
            transport['distance'],
            transport['duration'],
            datetime_to_seconds(start_time),
            datetime_to_seconds(end_time),
            start_time.hour,
            len(transport['trackPoints']),
            self._compute_total_distance(
                [fp['lon'], fp['lat']],
                [lp['lon'], lp['lat']],
                self.station_points)
        ]
        return np.array(features).reshape(1, -1)

    def _predict_transport_type(self, transport):
        """ Predicts the transportation type from a transport dict """
        X = self._create_features(transport)
        pred = self.model.predict(X)
        return LABELS[pred]

    def _compute_carbon(self, transport):
        """ Computes the kgs of carbon for a given transport
            TODO: move distance transformation to transform methods
        """
        url = BASE + TYPES[transport['type']]
        params = {
            'distance': transport['distance'] / 1000, # convert meters to kilometers
            'key': KEY
        }
        res = requests.get(url, params=params).json()
        kgs = res['decisions']['carbon']['object']['value']
        return kgs

    def run(self):
        transports = []
        for transport in self.transports:
            transport = self._transform_transport(transport)
            transport['type'] = self._predict_transport_type(transport)
            transport['carbon'] = self._compute_carbon(transport)
            transport['geojson'] = self._create_feature_collection(transport)
            transports.append(transports)
        return transports


if __name__ == '__main__':
    LIMIT = 5
    LABELS = ['subway', 'bus', 'car', 'airplane']
    client = pymongo.MongoClient('localhost', 27017)
    db = client.carbon

    # Get the user's profile
    user = db.users.find_one({'userId': 32734778124657154})
    moves = mvs.MovesClient(access_token=user['user']['access_token'])

    # Get the model
    model = pickle.load(open('../models/gradient_boosting.p', 'rb'))

    # Extract the transport segmentst
    extractor = ExtractMovesResource(user, db, 'moves2', moves)
    extractor.run()

    # Transform the transport records
    #transformer = TransformTransports(db, 'moves2', transports, model)
    #transformer.run()


