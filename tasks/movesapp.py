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
    the date the person joined Moves to yesterday (the last full day).
    """
    base_date = dateutil.parser.parse(start_date)
    yesterday = datetime.datetime.today() - datetime.timedelta(1)
    numdays = (yesterday - base_date).days
    for x in range(0, numdays):
        yield (yesterday - datetime.timedelta(days=x)).date()


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


#def transform_resource(resource, record_type, profile):
#    """ Adds metadata to a move source record.
#
#    NOTE: this is not used
#    """
#    date_datetime = dateutil.parser.parse(resource['date'])
#
#    if resource.has_key('lastUpdate'):
#        update_datetime = dateutil.parser.parse(resource['lastUpdate'])
#    else:
#        update_datetime = date_datetime
#
#    transformed = {
#        'userId': profile['userId'],
#        'record_type': record_type,
#        'last_update': update_datetime,
#        'date': date_datetime,
#        'data': resource
#    }
#    return transformed


def insert_resources(transformed_resources):
    """ Inserts a collection of transformed resources into
    the moves staging database.
    """
    try:
        res = db.moves2.insert(transformed_resources)
    except pymongo.errors.BulkWriteError as bwe:
        # res = db.moves2.remove(results)
        logging.error(bwe.details)
    except Exception, exception:
        logging.error(exception.message)
        res = None

    return res


def extract_segments(storylines):
    """ Extracts the segments from a list of storyline dicts

    Args:
        storyline: Moves storyline dict

    Returns:
        segments: list of Moves segment dicts

    TODO:
        I'll need to refactor as the "data" attribute
        can probably be removed
    """
    segments = [s.get('segments') for s in storylines
                if s.get('segments')]
    return itertools.chain(*segments)


def extract_activities(segments):
    """ Extracts the actiities from segments
    Args:
        segments: list of Moves segment dicts

    Returns:
        activities: list of activity dicts
    """
    activities = [s.get('activities') for s in segments
                  if s.has_key('activities')]
    return itertools.chain(*activities)


def extract_transports(activities):
    """ Extracts the transportation activities from user activities
    Args:
        activities: list of Moves activity dicts

    Returns:
        transports: list of transport activity dicts
    """
    return filter(lambda a: a.get('activity') in ('transport', 'airplane'), activities)


def transform_transport(transport):
    """ Updates time information for a transport dict """
    transport['date'] = dateutil.parser.parse(transport['startTime']).strftime("%Y-%m-%d")
    transport['startDatetime'] = dateutil.parser.parse(transport['startTime'])
    transport['endDatetime'] = dateutil.parser.parse(transport['endTime'])
    return transport


def transform_transports(transports):
    """ Updates time information for a list of transport """
    return map(transform_transport, transports)


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
    labels = ['subway', 'bus', 'car', 'airplane']
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


if __name__ == '__main__':
    LIMIT = 45
    client = pymongo.MongoClient('localhost', 27017)
    db = client.carbon

    # Get the user's profile
    user = db.users.find_one({'userId': 32734778124657154})
    moves = mvs.MovesClient(access_token=profile['user']['access_token'])

    # Get the model
    model = pickle.load(open('../models/gradient_boosting.p', 'rb'))

    # Find the subway station entrances
    subways_entrances = db.subway_entrances.find_one()
    features = subways_entrances['features']
    station_points = [p['geometry']['coordinates'] for p in features]

    # Find the dates that haven't been fetched
    membership_dates = active_daterange(user['profile']['firstDate'])
    existing_dates_ = existing_dates()
    no_transport_dates_ = no_transport_dates()
    missing_dates_ = missing_dates(
        membership_dates, existing_dates_, no_transport_dates_)

    for date in missing_dates_[:LIMIT]:
        storyline = fetch_resource('storyline', date)
        segments = extract_segments(storyline)
        activities = extract_activities(segments)
        transports = extract_transports(activities)
        transformed = transform_transports(transports)
        if not transformed:
            # add the date to the no transport dates and continue
            db.no_transport_dates.insert({'date': date.strftime('%Y-%m-%d')})
            continue
        # predict the transportation type
        preds = [predict_transport_type(t, model, station_points) for t in transformed]
        # add the predictions
        transports_type = [add_prediction(t, preds[ix]) for ix, t in enumerate(transformed)]
        # add the carbon
        transports_carbon = [add_carbon(t) for t in transports_type]
        # add the geojson
        transports_geojson = [add_feature_collection(t) for t in transports_carbon]
        # insert the transports
        insert_resources(transports_geojson)


