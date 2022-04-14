"""Request APIs

This script calls apis with a variety of authentication and paging
configurations using the requests module. Current configurations
include:

  * json_token_paged_count
  * json_user_pass_paged_next
  * json_user_pass_paged_count
  * json_user_pass_not_paged
  * json_api_key_not_paged
  * xml_user_pass_not_paged
  
current auth_type: token, user-pass, pass, api-key

use_params: True or False

Should a new api not conform to an existing configuration:

  1. Add new elif condition to api_by_response_type() function
  2. Create new function customized for api
  3. Update requests_post() and/or requests_get() as appropriate

Python Module Requirements:

 * requests
 * urllib3
 * ast
 * json
 * xml.etree.ElementTree
 * functools

Custom Module Requirements:

 * blob_functions.py
"""

from functools import wraps # for debugging function. Set DEBUG variable = True

from datetime import datetime

import requests
import ast # used to convert string into dictionary
import json
import xml.etree.ElementTree as ET # for processing xml responses

import urllib3 # to disable SSL warnings
urllib3.disable_warnings()

from blob_functions import blob_write

# Set DEBUG to true to return function arguments and return values.
DEBUG = False

# Print the argument names and values and return values for debugging.
def debug_log(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        if DEBUG:
            print(">> Called", function.__name__, "\n",
                {**dict(zip(function.__code__.co_varnames, args)), **kwargs})
        result = function(*args, **kwargs)
        if DEBUG:
            print(">>", function.__name__, "return:\n", result)
        return result
    return wrapper

# Find a key in a nested dictionary and return value.
@debug_log
def recursive_lookup(k, d):
    if k in d:
        return d[k]
    for v in d.values():
        if isinstance(v, dict):
            return recursive_lookup(k, v)
    return None

class ApiCall:
    @debug_log
    def __init__(
            self, aip_config_row, full_folder_name, access_token, password):
        self.api_type = aip_config_row['api_type']
        self.auth_type = aip_config_row['auth_type']
        self.use_params = aip_config_row['use_params']
        self.endpoint_name = aip_config_row['endpoint_name']
        self.full_url = aip_config_row['base_url'] \
            + aip_config_row['endpoint_url']
        self.additional_url_dict = ast.literal_eval(
            aip_config_row['additional_url_string'])  
        self.token_url = aip_config_row['token_url']
        self.user = aip_config_row['user']
        self.password = password
        self.access_token = access_token
        self.first_page_number = aip_config_row['first_page_number']
        self.total_pages_key_name = aip_config_row['total_pages_key_name']
        self.date_string = datetime.now().strftime("%Y%m%d")
        self.full_folder_name_value = full_folder_name
        
        self.success_response = self.api_by_response_type()
        
    # Determines which function to use based on criteria defined in the config 
    # file.
    @debug_log
    def api_by_response_type(self):
        if self.api_type == 'json_token_paged_count':
            success_response = self.json_token_paged_count()
            return success_response
        elif self.api_type == 'json_user_pass_paged_next':
            success_response = self.json_user_pass_paged_next()
            return success_response
        elif self.api_type == 'json_user_pass_paged_count':
            success_response = self.json_user_pass_paged_count()
            return success_response
        elif self.api_type == 'json_user_pass_not_paged':
            success_response = self.json_user_pass_not_paged()
            return success_response
        elif self.api_type == 'json_api_key_not_paged':
            success_response = self.json_api_key_not_paged()
            return success_response
        elif self.api_type == 'xml_user_pass_not_paged':
            success_response = self.xml_user_pass_not_paged()
            return success_response
        else:
            success_response = self.csv_user_pass_not_paged()
            return success_response        
        
    # Post to get api access token.
    @debug_log
    def requests_post(self):
        try:
            data = json.loads(self.access_token)
            response = requests.post(
                self.token_url, data=data, verify=False, 
                allow_redirects=False)
            response.raise_for_status()
            response = response.json()['access_token']
            return response
        except requests.exceptions.HTTPError as errh:
            success_response = 'HTTPError'
        except requests.exceptions.ConnectionError as errc:
            success_response = 'ConnectionError'
        except requests.exceptions.Timeout as errt:
            success_response = 'Timeout'
        except requests.exceptions.RequestException as err:
            success_response = 'RequestException'
        return success_response

    # Get api response given different authentication types and page types.
    @debug_log
    def requests_get(self, additional_url_dict = {}, headers = {}):
        try:
            # Api authentication is token.
            if self.auth_type == 'token':
                # Api requires additional parameters.
                if self.use_params == True: 
                    response = requests.get(
                        self.full_url, params=additional_url_dict, 
                        headers = headers, verify = False)                    
                    response.raise_for_status()
                    return response
                # Api requires no additional parameters.
                else: 
                    response = requests.get(
                        self.full_url, auth=(
                            self.user, 
                            self.password), 
                        verify = False)
                    response.raise_for_status()
                    return response
            # Api authentication is basic user/password.
            elif self.auth_type == 'user-pass': 
                if self.use_params == True:
                    response = requests.get(
                        self.full_url, params=additional_url_dict, 
                        auth=(self.user, self.password), verify = False)
                    response.raise_for_status()
                    return response
                else: 
                    response = requests.get(
                        self.full_url, auth=(self.user, self.password), 
                        verify = False)
                    response.raise_for_status()
                    return response
            # Api authentication is password only
            # TODO: need to test with api (xml)
            elif self.auth_type == 'pass':  
                if self.use_params == True:
                    response = requests.get(
                        self.full_url + self.password, 
                        params=additional_url_dict, verify = False)
                    response.raise_for_status()
                    return response
                else:
                    response = requests.get(
                        self.full_url + self.password, 
                        verify = False)
                    response.raise_for_status()
                    return response
            # Api authentication is api key
            elif self.auth_type == 'api-key':
                if self.use_params == True:
                    response = requests.get(
                        self.full_url, params=additional_url_dict, 
                        verify = False)
                    response.raise_for_status()
                    return response
                else:
                    response = requests.get(
                        self.full_url, verify = False)
                    response.raise_for_status()
                    return response
            else:
                print('auth type is unknown')  
                success_response = 'Success'
        except requests.exceptions.HTTPError as errh:
            print("An Http Error occurred:" + repr(errh))
            success_response = 'HTTPError'
        except requests.exceptions.ConnectionError as errc:
            #print("An Error Connecting to the API occurred:" + repr(errc))
            success_response = 'ConnectionError'
        except requests.exceptions.Timeout as errt:
            #print("A Timeout Error occurred:" + repr(errt))
            success_response = 'Timeout'
        except requests.exceptions.RequestException as err:
            #print("An Unknown Error occurred" + repr(err))
            success_response = 'RequestException'
        return success_response 
    
    # Process paged endpoints with token authentication and json response.
    @debug_log
    def json_token_paged_count(self):
        access_token_response = self.requests_post()
        headers = {'Authorization': 'Bearer ' + access_token_response}
        first_page_number = self.first_page_number
        response = self.requests_get(self.additional_url_dict, headers)
        if isinstance(response, str): # returned string indicates error
            return response
        else:
            num_pages = recursive_lookup(
                self.total_pages_key_name, response.json())
            additional_url_dict = self.additional_url_dict
            for page in range(int(first_page_number), num_pages):
                additional_url_dict['page'] = str(page)
                response = self.requests_get(additional_url_dict, headers)
                if isinstance(response, str):
                    return response            
                else:
                    body = json.dumps(response.json())
                    blob_write(
                        'application/json', self.full_folder_name_value, 
                        self.endpoint_name + '-' + self.date_string + '-page-' 
                        + str(page).rjust(3, '0') + '.json', body)
                    success_response = 'Success'
            return success_response

    # Process paged endpoints with username/password authentication and 
    # json response.
    @debug_log
    def json_user_pass_paged_count(self):
        self.additional_url_dict['page'] = self.first_page_number
        response = self.requests_get(
            additional_url_dict = self.additional_url_dict)
        if isinstance(response, str): # returned string indicates error
            return response
        else:
            response = response.json() 
            num_pages = int(response[self.total_pages_key_name])
            page = self.first_page_number
            body = json.dumps(response)
            blob_write(
                'application/json', self.full_folder_name_value, 
                self.endpoint_name + '-' + self.date_string + '-page-' 
                + str(page).rjust(3, '0') + '.json', body)
            for page in range(int(self.first_page_number), num_pages):
                self.additional_url_dict['page'] = str(page)
                response = self.requests_get(
                    additional_url_dict = self.additional_url_dict)
                if isinstance(response, str):
                    return response            
                else:
                    body = json.dumps(response.json())                
                    blob_write(
                        'application/json', self.full_folder_name_value, 
                        self.endpoint_name + '-' + self.date_string + '-page-' 
                        + str(page).rjust(3, '0') + '.json', body)
                    success_response = 'Success'
            return success_response  

    # Process non-paged endpoints with username/password authentication and 
    # json response.
    @debug_log
    def json_user_pass_not_paged(self):
        response = self.requests_get(additional_url_dict = self.additional_url_dict)
        if isinstance(response, str): # returned string indicates error
            return response
        else:
            response = response.json() 
            body = json.dumps(response)
            blob_write(
                'application/json', self.full_folder_name_value, 
                self.endpoint_name + '-' + self.date_string 
                + '.json', body)
            success_response = 'Success'
        return success_response

    # Process non-paged endpoints with api key authentication and json response.
    @debug_log
    def json_api_key_not_paged(self):
        self.additional_url_dict['registrationkey'] = self.password
        response = self.requests_get(additional_url_dict = self.additional_url_dict)
        if isinstance(response, str): # returned string indicates error
            return response
        else:
            response = response.json() 
            body = json.dumps(response)
            blob_write(
                'application/json', self.full_folder_name_value, 
                self.endpoint_name + '-' + self.date_string 
                + '.json', body)
            success_response = 'Success'
        return success_response  

    # Process paged endpoints with username/password authentication with 
    # a "next" url returned, and json response.
    @debug_log
    def json_user_pass_paged_next(self):
        next_page_exists = True
        page = 1
        response = self.requests_get(additional_url_dict = self.additional_url_dict)
        if isinstance(response, str): # returned string indicates error
            return response
        else:
            response = response.json()
            try:
                next_page = response['d']['__next']
            except KeyError:
                next_page_exists = False
            body = json.dumps(response)
            blob_write(
                'application/json', self.full_folder_name_value, self.endpoint_name 
                + '-' + self.date_string + '-page-' + str(page).rjust(3, '0') 
                + '.json', body)
            while next_page_exists == True:
                page += 1
                self.full_url = next_page
                response = self.requests_get(
                    additional_url_dict = self.additional_url_dict)
                if isinstance(response, str):
                    return response
                else:
                    response = response.json()
                    body = json.dumps(response)
                    blob_write(
                        'application/json', self.full_folder_name_value, 
                        self.endpoint_name + '-' + self.date_string + '-page-' 
                        + str(page).rjust(3, '0') + '.json', body)
                    success_response = 'Success'
                    try:
                        next_page = response['d']['__next']
                    except KeyError:
                        next_page_exists = False   
            return success_response
        
    # Process non-paged endpoints with username/password authentication and 
    # xml response. 
    # TODO: refactor to avoid writing response locally before to blob
    # I tried a number of methods uncessessfully to write response directly to 
    # blob storage.
    @debug_log
    def xml_user_pass_not_paged(self):
        response = self.requests_get()
        if isinstance(response, str): # returned string indicates error
            return response
        else:
            tree = ET.ElementTree(ET.fromstring(response.content))
            tree.write('files/' + self.endpoint_name + '-' + self.date_string + '.xml')
            with open(
                'files/' + self.endpoint_name + '-' 
                + self.date_string + '.xml', 'rb') as body:
                blob_write(
                    'application/xml', self.full_folder_name_value, 
                    self.endpoint_name + '-' + self.date_string 
                    + '.xml', body)
                success_response = 'Success'
        return success_response