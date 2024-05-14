# File: libs/azure/functions/blueprints/esquire/xandr/activities/standard.py
# all the code related to segments and segment manipulation

from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
from libs.data import from_bind
from sqlalchemy.orm import Session
import json, geojson, os, pandas as pd, logging
import boto3, time, botocore, random, os, requests, json, re

bp: Blueprint = Blueprint()


# activity to grab the geojson data and format the request files for OnSpot
@bp.activity_trigger(input_name="ingress")
def activity_onspotSegmentsCreate_segments(ingress: dict,xandr_advertiser_id, segment_name, xandr_expire_minutes):
        #initially set segment_found as False
        # segment_found = False
        # #search for an existing segment of the same name, return the id if found
        # segment_search_url = f"https://api.appnexus.com/segment?search={segment_name}"
        # #print(segment_search_url)
        # segment_search_headers = {'Authorization': token}
        # searchresponse = requests.get(segment_search_url, headers=segment_search_headers)
        # #if the authentication token has expired... create a new one and re-run
        # if searchresponse.status_code !=200:
        #     if searchresponse.json()['response']['error_id']=='NOAUTH':
        #         print('Resetting auth token')
        #         #reset token using Xandr api call
        #         get_auth_token()
        #         update_auth_from_ssm(self.token)
        #         segment_search_headers = {'Authorization': self.token}
        #         searchresponse = requests.get(segment_search_url, headers=segment_search_headers)
        # print('SEGMENT SEARCH RESPONSE: {}'.format(searchresponse.json()))
        
        
        # if searchresponse.json()['response']['count'] > 0:
        #     segments = searchresponse.json()['response'].get('segments', [])
        #     segment =  [x for x in segments if x['code']==segment_name]
        #     #print("Check for existing segment\n")
        #     segment_found = len(segment) >0
        #     if segment_found: 
        #         print("Found existing segment!   code :{}".format(segment[0]['code']))
        #         return segment[0]['id']
        # if not segment_found: 
        #     #create a new segment based on the filename
        #     segment_url = f"https://api.appnexus.com/segment?advertiser_id={xandr_advertiser_id}"
        #     segment_headers = {'Content-Type': 'application/json', 'Authorization': self.token}
        #     segment_json = {"segment":{"advertiser_id": xandr_advertiser_id, "code":segment_name, "description": segment_name, "expire_minutes": xandr_expire_minutes, "member_id": self.member_id, "short_name": segment_name, "state": 'active'}}
        #     jsonData = json.dumps(segment_json)
        #     print(jsonData)
        #     response = requests.post(segment_url, data=jsonData, headers=segment_headers)
        #     print('NEW SEGMENT: {}'.format(response.json()))
        #     segment_id = response.json()['response']['id']
        #     return segment_id    
    return {}