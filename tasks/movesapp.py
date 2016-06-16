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

def existing_dates_(profile, record_type):
    """ Finds the earliest update for a moves record. """
    docs = db.moves2.find({
        'record_type': record_type,
        'userId': profile['userId']
        }, {'date': 1})
    dates = [doc['date'].date() for doc in docs]
    return dates


def service_daterange(start_date):
    """ Creates a list of datatime date objects from starting with
    the date the person joined Moves to today.
    """
    base_date = dateutil.parser.parse(start_date)
    today = datetime.datetime.today()
    numdays = (today - base_date).days
    dates = [(today - datetime.timedelta(days=x)).date()
                for x in range(0, numdays)]
    return dates


def missing_dates(service_dates, existing_dates):
    """ Returns a list of dates that haven't been fetched """
    return [date for date in service_dates if date not in existing_dates]


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


def fetch_resources(resource_type, missing_dates):
    resources = []
    for date in missing_dates[:30]:
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
    segments = [s['data']['segments'] for s in storylines
                if s['data']['segments']]
    return itertools.chain(*segments)


def extract_activities(segments):
    activities = [s['activities'] for s in segments
                  if s.has_key('activities')]
    return itertools.chain(*activities)


def datetime_to_seconds(dt):
    return (dt.hour * 3600) + (dt.minute * 60) + dt.second


def create_features(transport, station_points):
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
    distances = [distance_on_unit_sphere(lat_lng[1], lat_lng[0],
                    point[1], point[0]) for point in station_points]
    return min(distances)


def compute_total_distance(first_point, last_point, station_points):
    start = compute_min_distance(first_point, station_points)
    end = compute_min_distance(last_point, station_points)

    return start + end


def distance_on_unit_sphere(lat1, long1, lat2, long2):
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
    X = create_features(transport, station_points)
    pred = model.predict(X)
    return labels[pred]


def predict_transport_types(transports, model, station_points):
    X = np.array([create_features(t, station_points) for t in transports])
    preds = model.predict(X)
    return [labels[pred] for pred in preds]


def compute_carbon_kg(transport):
    url = BASE + TYPES[transport['type']]
    params = {
        'distance': transport['distance'],
        'key': KEY
    }
    res = requests.get(url, params=params).json()
    kgs = res['decisions']['carbon']['object']['value']

    return kgs


def add_carbon(transport):
    time.sleep(.05)
    kgs = compute_carbon_kg(transport)
    transport['carbon'] = kgs
    return transport


def add_prediction(transport, prediction):
    transport['type'] = prediction
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

    # for i in range(20):
    # print "round {}".format(i)`
    # Find the dates that haven't been fetched
    membership_dates = service_daterange(profile['profile']['firstDate'])
    existing_dates = existing_dates_(profile, 'storyline')
    non_existing_dates = missing_dates(membership_dates, existing_dates)

    # Fetch the data for the missing dates
    resources = fetch_resources('storyline', non_existing_dates)

    # Transform the date into a collection of activiy records
    transformed_resources = list(transform_resources(resources, 'storyline', profile))
    segments = list(extract_segments(transformed_resources))
    activities = list(extract_activities(segments))
    transports = [a for a in activities if a['activity'] == 'transport']

    # Predict the carbon
    preds = [predict_transport_type(transport, model, station_points) for transport in transports]
    transports_with_type = [add_prediction(t, preds[ix]) for ix, t in enumerate(transports)]
    transports_with_carbon = [add_carbon(t) for t in transports_with_type]
    insert_resources(transports_with_carbon)





