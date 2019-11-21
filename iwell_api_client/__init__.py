#!/usr/bin/env python3

import json
import logging
import os
import requests

logger = logging.getLogger()
logger.setLevel(os.getenv('LOG_LEVEL', logging.INFO))

class IWell():

  def __init__(self, client_id, client_secret, username, password, url='https://api.iwell.info'):
    self.session = requests.Session()
    self.url = url
    self.client_id = client_id
    self.client_secret = client_secret
    self.username = username
    self.password = password
    self.auth_token = self._get_auth_token()

  def _get_auth_token(self):
    logger.debug('Getting iWell oAuth token')
    params = {
        'grant_type': 'password',
        'client_id': self.client_id,
        'client_secret': self.client_secret,
        'username': self.username,
        'password': self.password
    }

    resp = self.session.post(f'{self.url}/v1/oauth2/access-token', json=params)
    if resp.status_code == 200:
      logger.debug(f'Auth response: {resp.json()}')
      return resp.json()['access_token']
    else:
      raise Exception(f'Unable to authenticate to iWell API: {resp.text}')

  def _get(self, path, params={}):
    url = f'{self.url}/{path}'
    logger.debug(f'Getting {url} with parameters: {params}')
    headers = {
        'Authorization': f'Bearer {self.auth_token}'
    }
    resp = self.session.get(url, headers=headers, params=params)
    if resp.status_code == 200:
      logger.debug(f'Response: {resp.json()}')
      return resp.json()
    else:
      raise Exception(f'Unable to get from iWell API: {resp.text}')

  def _post(self, path, data, params={}):
    url = f'{self.url}/{path}'
    logger.debug(f'Posting {data} to {url} with parameters: {params}')
    headers = {
        'Authorization': f'Bearer {self.auth_token}'
    }
    resp = self.session.post(url, json=data, headers=headers, params=params)
    if resp.status_code == 201:
      logger.debug(f'Response: {resp.json()}')
      return resp.json()
    else:
      error = resp.json()['error']['message']
      raise Exception(error)

  def list_wells(self, since=None):
    path = 'v1/wells'
    if since:
      return self._get(path, params={'since': since})
    else:
      return self._get(path)

  def list_fields(self, since=None):
    path = 'v1/fields'
    if since:
      return self._get(path, params={'since': since})
    else:
      return self._get(path)

  def list_well_field_values(self, well_id, field_id, start=None, end=None, since=None):
    path = f'v1/wells/{well_id}/fields/{field_id}/values'
    params = {}
    for param in ['start', 'end', 'since']:
      if param in locals() and locals()[param]:
        params[param] = locals()[param]
    return self._get(path, params)

  def create_well_production(self, well_id, data):
    path = f'v1/wells/{well_id}/production'
    return self._post(path, data)

  def create_well_field_value(self, well_id, field_id, data):
    path = f'v1/wells/{well_id}/fields/{field_id}/values'
    return self._post(path, data)