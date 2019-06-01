from __future__ import absolute_import

import argparse
import sys
import os
import json
import pandas as pd
from pandas.io.json import json_normalize
import copy
from pandas.tseries.offsets import Day
import time

from googleapiclient import discovery
from googleapiclient.http import build_http
from googleapiclient.errors import HttpError

def init(name, version, filename, scope=None):
    try:
        from oauth2client import client
        from oauth2client import file
        from oauth2client import tools
    except ImportError:
        raise ImportError('googleapiclient.sample_tools requires oauth2client. Please install oauth2client and try again.')

    if scope is None:
        scope = 'https://www.googleapis.com/auth/' + name

    client_secrets = os.path.join(os.path.dirname(filename),
                                  'client_secrets.json')

    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(client_secrets,
                                          scope=scope,
                                          message=tools.message_if_missing(client_secrets))

    # Prepare credentials, and authorize HTTP object with them.
    # If the credentials don't exist or are invalid run through the native client
    # flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.
    storage = file.Storage(name + '.dat')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http=build_http())

    # Construct a service object via the discovery service.
    service = discovery.build(name, version, http=http)

    return service

def main(client_domain, rowLimit, startRow, which_date):
    service = init('webmasters', 'v3', __file__,
        scope='https://www.googleapis.com/auth/webmasters.readonly')

    request = {
        'startDate': which_date,
        'endDate': which_date,
        'dimensions': ['query', 'date', 'page'],
        'rowLimit': rowLimit,
        'startRow': startRow
    }

    print 'Display results for startRow = ', startRow, ": "

    return execute_request(service, client_domain, request)

def execute_request(service, property_uri, request):
    return service.searchanalytics().query(
        siteUrl=property_uri, body=request).execute()

def convert_response_to_df(response):
    # split query, date, page columns from api keys
    df = pd.DataFrame(response['rows'], columns=['keys', 'impressions', 'clicks', 'position'])

    df.keys = df['keys'].astype(str)
    df['keys'] = df['keys'].astype(str).str.replace('\"', '\'')
    df['keys'] = df['keys'].astype(str).str.replace('\[u\'', '')
    df['keys'] = df['keys'].astype(str).str.replace('\'\]', '')

    expand = df['keys'].astype(str).str.split('\'\,\ u\'', n=2, expand=True)
    df['query'] = expand[0]
    df['date'] = expand[1]
    df['page'] = expand[2]
    df.drop(columns=['keys'], inplace=True)

    # position column into integer
    df['position'] = df['position'].astype(int)

    # generate which_page column based on position
    import math

    df['temp'] = df.position / 10
    df['which_page'] = df.temp.apply(math.ceil)
    df.drop(columns=['temp'], axis=1, inplace=True)

    print df.head(3)

    return df


def request_for_one_day(client_domain, current_date):

    client_domain = client_domain

    which_day = current_date

    rowLimit = 25000
    startRow = 0
    i = 0

    response = main(client_domain, rowLimit, startRow, which_day)
    df = convert_response_to_df(response)
    response_append = copy.copy(response)

    while 'rows' in response_append:
        i += 1
        startRow = i * rowLimit
        try:
            response_append = main(client_domain, rowLimit, startRow, which_day)
        except HttpError as err:
            if err.resp.get('content-type', '').startswith('application/json'):
                error_code = json.loads(err.content).get('error').get('code')
                message = json.loads(err.content).get('error').get('message')
                reason = json.loads(err.content).get('error').get('errors')[0].get('reason')
                print "error code: ", error_code, "\n", reason, "\n", message
                time.sleep(905)
                i = i - 1
        else:
            if 'rows' in response_append:
                df_temp = convert_response_to_df(response_append)
                df = pd.concat([df, df_temp], sort=True, ignore_index=True)

    df.drop_duplicates(keep='first', inplace=True)
    return df[['query', 'date', 'page', 'impressions', 'clicks', 'position', 'which_page']]


def loop_over_dates(client_domain, start_date, end_date):
    until_date = end_date
    which_date = start_date
    current_date = copy.copy(until_date)

    current_date = pd.to_datetime(current_date)
    which_date = pd.to_datetime(which_date)

    df_all_dates = pd.DataFrame()

    while (current_date != which_date - Day()):

        current_date = current_date.strftime('%Y-%m-%d')

        try:
            response_one_day = request_for_one_day(client_domain, current_date)
            time.sleep(1)
        except HttpError as err:
            if err.resp.get('content-type', '').startswith('application/json'):
                error_code = json.loads(err.content).get('error').get('code')
                reason = json.loads(err.content).get('error').get('errors')[0].get('reason')
                message = json.loads(err.content).get('error').get('errors')[0].get('message')
                print "error code: ", error_code, "\n", reason, "\n", message
        else:

            # temp = convert_response_to_df(response_one_day)
            print 'response_one_day!!!!!!!!'
            print response_one_day.head(3)
            print list(response_one_day)
            # print 'df_all_dates !!!!!'
            # print df_all_dates.head(3)
            # print list(df_all_dates)
            df_all_dates = pd.concat([df_all_dates, response_one_day])

        current_date = pd.to_datetime(current_date)
        current_date -= Day()

    return df_all_dates




if __name__ == '__main__':
    title = 'test GSC Raw Output Dates Loop'
    client_domain = 'http://**'

    start_date = '2019-03-15'
    end_date = '2019-03-30'

    df_all = loop_over_dates(client_domain, start_date, end_date)

    with pd.ExcelWriter(title + '.xlsx') as writer:
        df_all.to_excel(writer, sheet_name=title, index=False)