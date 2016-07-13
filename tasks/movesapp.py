#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Module for extracting Moves data """

import datetime
import dateutil.parser
import itertools
import logging
import pickle
import math
import requests
import time

import pymongo
import numpy as np
import moves as mvs


KEY = '20f32c9fad4aebc9998f8ce569bdc358'
BASE = 'http://impact.brighterplanet.com/'
TYPES = {
 'car': 'automobile_trips.json',
 'subway': 'rail_trips.json?class=commuter',
 'airplane': 'flights.json',
 'bus': 'bus_trips.json'
}

def existing_dates():
    """ Finds the earliest update for a moves record. """
    docs = db.moves2.distinct('date')
    dates = [dateutil.parser.parse(doc).date() for doc in docs]
    return dates


def no_transport_dates():
    """ Finds the dates that don't have transport records """
    docs = db.no_transport_dates.distinct('date')
    dates = [dateutil.parser.parse(doc).date() for doc in docs]
    return dates


def create_no_transport_dates(no_transport_dates):
    """ Creates a list of no_transport_date dicts """
    return [{'date': d.strftime('%Y-%m-%d')} for d in no_transport_dates]


def active_daterange(start_date):
    """ Creates a list of datatime date objects from starting with
    the date the person joined Moves to today.
    """
    base_date = dateutil.parser.parse(start_date)
    today = datetime.datetime.today()
    numdays = (today - base_date).days
    dates = [(today - datetime.timedelta(days=x)).date()
                for x in range(0, numdays)]
    return dates


def missing_dates(service_dates, existing_dates, no_transport_dates):
    """ Returns a list of dates that haven't been fetched """
    return [date for date in service_dates
            if date not in existing_dates
            and date not in no_transport_dates]


def fetch_resource(resource, date, update_since=None):
    """ Fetches a user's Moves summary for a given date range

    Args:
        resource: String of the moves resource to fetch.
        start_date: String of the start date.
        end_date: String of the end date.

    Returns:
        resources: List of resouce dicts from the Moves API

    Raises:
        ValueError: resource requested is not a moves resource.
    """
    if resource not in ['summary', 'activities', 'places', 'storyline']:
        raise ValueError('Invalid Moves resource.')

    rsrc_path = "user/{}/daily/{}?".format(resource, date)

    if resource == 'storyline':
        rsrc_path = "%s&trackPoints=true" % rsrc_path

    if update_since:
        rsrc_path = "%s&updateSince>T%sZ" % (rsrc_path, update_since)

    try:
        resources = moves.api(rsrc_path, 'GET').json()
    except Exception, exception:
        logging.error(exception.message)
        raise

    return resources


def fetch_resources(resource_type, missing_dates, limit):
    """ Fetches resources of a given type for a list of dates """
    resources = []
    for date in missing_dates[:limit]:
        resource = fetch_resource(resource_type, date)
        resources.append(resource[0])
    return resources


def transform_resource(resource, record_type, profile):
    """ Adds metadata to a move source record. """
    date_datetime = dateutil.parser.parse(resource['date'])

    if resource.has_key('lastUpdate'):
        update_datetime = dateutil.parser.parse(resource['lastUpdate'])
    else:
        update_datetime = date_datetime

    transformed = {
        'userId': profile['userId'],
        'record_type': record_type,
        'last_update': update_datetime,
        'date': date_datetime,
        'data': resource
    }
    return transformed


def transform_resources(resources, record_type, profile):
    """ Adds some metadata to raw Moves resources. """
    for resource in resources:
        yield transform_resource(resource, record_type, profile)


def insert_resources(transformed_resources):
    """ Inserts a collection of transformed resources into
    the moves staging database.
    """
    try:
        res = db.moves2.insert(transformed_resources)
    except pymongo.errors.BulkWriteError, results:
        res = db.moves2.remove(results)
        logging.error('BulkWriteError')
    except Exception, exception:
        logging.error(exception.message)
        res = None

    return res


def extract_segments(storylines):
    """ Extracts segment lists from storyline dicts """
    segments = [s['data']['segments'] for s in storylines
                if s['data']['segments']]
    return itertools.chain(*segments)


def extract_activities(segments):
    """ Returns the activity dicts from a segment """
    activities = [s['activities'] for s in segments
                  if s.has_key('activities')]
    return itertools.chain(*activities)


def datetime_to_seconds(dt):
    """ Converts datetime to seconds """
    return (dt.hour * 3600) + (dt.minute * 60) + dt.second


def create_features(transport, station_points):
    """ Creates the features for use in the model """
    start_time = dateutil.parser.parse(transport['startTime'])
    end_time = dateutil.parser.parse(transport['endTime'])
    fp = transport['trackPoints'][0]
    lp = transport['trackPoints'][-1]
    feats = [
        transport['distance'],
        transport['duration'],
        datetime_to_seconds(start_time),
        datetime_to_seconds(end_time),
        start_time.hour,
        len(transport['trackPoints']),
        compute_total_distance(
            [fp['lon'], fp['lat']],
            [lp['lon'], lp['lat']],
            station_points)
    ]
    return np.array(feats).reshape(1, -1)


def compute_min_distance(lat_lng, station_points):
    """
    Computes the smallest distance between the start a point
    and a subway entrance.
    """
    distances = [distance_on_unit_sphere(lat_lng[1], lat_lng[0],
                    point[1], point[0]) for point in station_points]
    return min(distances)


def compute_total_distance(first_point, last_point, station_points):
    """
    Computes distance between the smallest starting and end point
    of a given transport.
    """
    start = compute_min_distance(first_point, station_points)
    end = compute_min_distance(last_point, station_points)

    return start + end


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

    # For two locations in spherical coordinates
    # (1, theta, phi) and (1, theta, phi)
    # cosine( arc length ) =
    #    sin phi sin phi' cos(theta-theta') + cos phi cos phi'
    # distance = rho * arc length

    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) +
           math.cos(phi1)*math.cos(phi2))
    arc = math.acos(cos)

    # Remember to multiply arc by the radius of the earth
    # in your favorite set of units to get length.
    earth_rad_miles = 3963.1676
    earth_rad_feet = earth_rad_miles

    return arc * earth_rad_feet


def predict_transport_type(transport, model, station_points):
    """ Predicts the transportation type from a transport dict """
    X = create_features(transport, station_points)
    pred = model.predict(X)
    return labels[pred]


def predict_transport_types(transports, model, station_points):
    """ Predicts the transportation types from a transport dict """
    X = np.array([create_features(t, station_points) for t in transports])
    preds = model.predict(X)
    return [labels[pred] for pred in preds]


def compute_carbon_kg(transport):
    """ Computes the kgs of carbon for a given transport """
    url = BASE + TYPES[transport['type']]
    params = {
        'distance': transport['distance'] / 1000, # convert meters to kilometers
        'key': KEY
    }
    res = requests.get(url, params=params).json()
    kgs = res['decisions']['carbon']['object']['value']

    return kgs


def add_carbon(transport):
    """ Adds carbon kg value attribute to transport """
    time.sleep(.05)
    kgs = compute_carbon_kg(transport)
    transport['carbon'] = kgs
    return transport


def add_prediction(transport, prediction):
    """ Adds the prediction attribute to a transport dict """
    transport['type'] = prediction
    return transport


def update_times(transport):
    """ Converts start and end times and to datetimes """
    transport['date'] = dateutil.parser.parse(transport['startTime']).strftime("%Y-%m-%d")
    transport['startDatetime'] = dateutil.parser.parse(transport['startTime'])
    transport['endDatetime'] = dateutil.parser.parse(transport['endTime'])
    return transport


def create_transport(activity):
    """ Creates a transport record """
    pass


def make_geometry(trackPoint):
    """ Makes a geojson geometry """
    return {
        'type': 'Point',
        'coordinates': [trackPoint['lon'], trackPoint['lat']]
    }


def make_property(trackPoint):
    """ Makes a geojson property dict """
    return {
        'latitude': trackPoint['lat'],
        'longitude': trackPoint['lon'],
        'time': trackPoint['time'],
        'id': 'transport'
    }


def make_feature(trackPoint):
    """ Makes a geojson feature dict """
    return {
        'type': 'Feature',
        'properties': make_property(trackPoint),
        'geometry': make_geometry(trackPoint)
    }


def make_feature_collection(transport):
    """ makes a geojson feature collection dict """
    return {
        'type': 'FeatureCollection',
        'features': [make_feature(tp) for tp in transport['trackPoints']]
    }


def add_feature_collection(transport):
    """ Adds feature collection dict to a transport dict """
    transport['geojson'] = make_feature_collection(transport)
    return transport



# SOME TEST STUFF TO SEE HOW THINGS ARE WORKING
if __name__ == '__main__':
    client = pymongo.MongoClient('localhost', 27017)
    db = client.carbon_calculator

    # Get the user's profile
    profile = db.users.find_one({'userId': 32734778124657154})
    moves = mvs.MovesClient(access_token=profile['user']['access_token'])

    # Get the model
    labels = ['subway', 'bus', 'car', 'airplane']
    model = pickle.load(open('../models/gradient_boosting.p', 'rb'))

    # Find the subway station entrances
    subways_entrances = db.subway_entrances.find_one()
    features = subways_entrances['features']
    station_points = [p['geometry']['coordinates'] for p in features]

    # Find the dates that haven't been fetched
    membership_dates = active_daterange(profile['profile']['firstDate'])
    existing_dates_ = existing_dates()
    no_transport_dates_ = no_transport_dates()
    non_existing_dates = missing_dates(
        membership_dates, existing_dates_, no_transport_dates_)

    # Fetch the data for the missing dates
    limit = 30
    resources = fetch_resources('storyline', non_existing_dates, limit)

    # Transform the date into a collection of activiy records
    transformed_resources = list(transform_resources(resources, 'storyline', profile))
    segments = list(extract_segments(transformed_resources))
    activities = list(extract_activities(segments))
    transports = [a for a in activities if a['activity'] == 'transport']
    transports_with_time = [update_times(t) for t in transports]
    print "Transport count is {}".format(len(transports))

    # Get the transport dates
    # Extract the transport dates that aren't in
    transport_dates = set([dateutil.parser.parse(t['date']).date()
        for t in transports_with_time])
    no_transport_dates_ = [date for date in non_existing_dates[:limit]
        if date not in transport_dates]
    no_transport_dates_ = create_no_transport_dates(no_transport_dates_)
    # Insert the no_transport_dates into the collection
    print "No transport count is {}".format(len(no_transport_dates_))
    try:
        db.no_transport_dates.insert(no_transport_dates_)
    except:
        pass

    # Predict the carbon
    preds = [predict_transport_type(transport, model, station_points) for transport in transports_with_time]
    transports_with_type = [add_prediction(t, preds[ix]) for ix, t in enumerate(transports)]
    transports_with_carbon = [add_carbon(t) for t in transports_with_type]
    transports_with_geojson = [add_feature_collection(t) for t in transports_with_carbon]
    insert_resources(transports_with_geojson)

