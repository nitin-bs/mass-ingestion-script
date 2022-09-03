import argparse
import inspect
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
import traceback
import urllib3
from colorama import Fore, Style

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
script_name = inspect.getfile(inspect.currentframe())
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.precision', 3)
import sys
import subprocess
import pkg_resources


required = {'pycryptodomex'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
skipped_tables=[]
if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

from Cryptodome.Cipher import AES
import base64
from Cryptodome.Protocol.KDF import PBKDF2
from Cryptodome.Hash import SHA256

cwd = os.getcwd()
configuration_file= open(f"{cwd}/conf/configurations.json","r")
configuration_json=json.load(configuration_file)

prev_header_count=configuration_json.get("file_properties",{}).get("header_rows_count",1)
refresh_token=''
delegation_token=''
#proxy_host = "{}://{}".format(IWUtils.get_value_from_file_or_default("proxy_server_scheme", "http"),IWUtils.get_proxy_host())
#proxy_port = IWUtils.get_proxy_port()
proxy_host=f"{configuration_json.get('protocol','http')}://{configuration_json.get('host','localhost')}"
proxy_port=f"{configuration_json.get('port','3001')}"

class CustomError(Exception):
    def __init__(self, message):
        self.message = message
        super(CustomError, self).__init__(self.message)

def callurl(url):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    try:
        print("url {url}".format(url=url))
        response = requests.request("GET", url, headers=headers,verify=False)
        if response is not None:
            return response
    except:
        raise CustomError("Unable to get response for url: {url}".format(url=url))

def print_success(message):
    print(Fore.GREEN + message)
    print(Style.RESET_ALL)

def print_error(message):
    print(Fore.RED + message)
    print(Style.RESET_ALL)

def trim_spaces(item):
    return item.strip()

def refresh_delegation_token():
    global delegation_token
    global refresh_token
    url = "{ip}:{port}/v3/security/token/access/".format(ip=proxy_host, port=proxy_port)
    headers = {
    'Authorization': 'Basic ' + refresh_token,
    'Content-Type': 'application/json'
    }
    response = requests.request("GET", url, headers=headers,verify=False)
    delegation_token = response.json().get("result").get("authentication_token")
    print(delegation_token)

def token_valid():
    global delegation_token
    url = "{ip}:{port}/v3/security/token/validate".format(ip=proxy_host, port=proxy_port)
    headers = {
    'Authorization': 'Bearer ' + delegation_token,
    'Content-Type': 'application/json'
    }
    response = requests.request("GET", url, headers=headers,verify=False)
    validity_status = response.json().get("result").get("is_valid")
    #print("token validity_status: ",validity_status)
    logging.info(f"token validity_status: {validity_status}")
    return validity_status


def aes_decrypt(encrypted_data):
    try:
        if len(encrypted_data.strip(" ")) == 0:
            return ""
        password = "infoworks"
        ciphertext  = base64.b64decode(encrypted_data)
        salt = ciphertext[0:16]
        iv = ciphertext[16:28]
        auth_tag = ciphertext[-16:]
        text = ciphertext[28:-16]
        key = PBKDF2(password, salt, dkLen = 256,count=65536,hmac_hash_module=SHA256)
        key = key[0:32]
        cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
        decrypttext = cipher.decrypt_and_verify(text,auth_tag)
        return decrypttext.decode('utf-8').strip("\n")
    except Exception as e:
        #print("failed to decode with aes/gcm/nopadding, defaulting to old method")
        #return aes_decrypt(encrypted_data)
        raise


def get_environment_id_from_name(environment_name):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    get_environment_name_base = "{ip}:{port}/v3/admin/environment".format(ip=proxy_host, port=proxy_port)
    filter_condition = "?filter={\"name\":\""+environment_name+"\"}"
    get_environment_name_url = get_environment_name_base + filter_condition
    try:
        response = requests.request('GET', get_environment_name_url, headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to find environment id corresponding to name {environment_name}")
            logging.error(f"Failed to find environment id corresponding to name {environment_name}")
            print(response.text)
            logging.info(response)
            sys.exit(-100)
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                return result[0]["id"]
            else:
                print_error(f"Failed to find environment id corresponding to name {environment_name}")
                logging.error(f"Failed to find environment id corresponding to name {environment_name}")
                sys.exit(-100)
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def get_environment_default_warehouse(environment_id):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    get_environment_warehouse_url = "{ip}:{port}/v3/admin/environment/{environment_id}".format(ip=proxy_host, port=proxy_port,environment_id=environment_id)
    try:
        response = requests.request('GET', get_environment_warehouse_url, headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to find default warehouse for environment {environment_id}")
            logging.error(f"Failed to find default warehouse for environment {environment_id}")
            print(response.text)
            logging.info(response)
            #sys.exit(-100)
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                return result["data_warehouse_configuration"]["warehouse"][0]["name"]
            else:
                print_error(f"Failed to find default warehouse for environment {environment_id}")
                logging.error(f"Failed to find default warehouse for environment {environment_id}")
                #sys.exit(-100)
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def get_environment_storage_id_from_name(environment_storage_name,environment_id):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    get_environment_storage_name_base = "{ip}:{port}/v3/admin/environment/{environment_id}/environment-storage".format(ip=proxy_host, port=proxy_port,environment_id=environment_id)
    filter_condition = "?filter={\"name\":\""+environment_storage_name+"\"}"
    get_environment_storage_name_url = get_environment_storage_name_base + filter_condition
    print(get_environment_storage_name_url)
    try:
        response = requests.request('GET', get_environment_storage_name_url, headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to find environment storage id corresponding to name {environment_id}")
            logging.error(f"Failed to find environment storage id corresponding to name {environment_id}")
            print(response.text)
            logging.info(response)
            sys.exit(-100)
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                return result[0]["id"]
            else:
                print_error(f"Failed to find environment storage id corresponding to name {environment_id}")
                logging.error(f"Failed to find environment storage id corresponding to name {environment_id}")
                sys.exit(-100)
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def get_interactive_compute_id_from_name(environment_compute_name,environment_id):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    get_environment_compute_name_base = "{ip}:{port}/v3/admin/environment/{environment_id}/environment-interactive-clusters".format(ip=proxy_host, port=proxy_port,environment_id=environment_id)
    filter_condition = "?filter={\"name\":\""+environment_compute_name+"\"}"
    get_environment_compute_name_url = get_environment_compute_name_base + filter_condition
    try:
        response = requests.request('GET', get_environment_compute_name_url, headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to find environment compute id corresponding to id {environment_id}")
            logging.error(f"Failed to find environment compute id corresponding to id {environment_id}")
            print(response.text)
            logging.info(response)
            sys.exit(-100)
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                return result[0]["id"]
            else:
                print_error(f"Failed to find environment compute id corresponding to id {environment_id}")
                logging.error(f"Failed to find environment compute id corresponding to id {environment_id}")
                return None
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def get_environment_compute_id_from_name(environment_compute_name,environment_id):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    interactive_compute_id=get_interactive_compute_id_from_name(environment_compute_name,environment_id)
    if interactive_compute_id:
        return str(interactive_compute_id)
    get_environment_compute_name_base = "{ip}:{port}/v3/admin/environment/{environment_id}/environment-compute-template".format(ip=proxy_host, port=proxy_port,environment_id=environment_id)
    filter_condition = "?filter={\"name\":\""+environment_compute_name+"\"}"
    get_environment_compute_name_url = get_environment_compute_name_base + filter_condition
    try:
        response = requests.request('GET', get_environment_compute_name_url, headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to find environment compute id corresponding to id {environment_id}")
            logging.error(f"Failed to find environment compute id corresponding to id {environment_id}")
            print(response.text)
            logging.info(response)
            sys.exit(-100)
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                return result[0]["id"]
            else:
                print_error(f"Failed to find environment compute id corresponding to id {environment_id}")
                logging.error(f"Failed to find environment compute id corresponding to id {environment_id}")
                sys.exit(-100)
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def get_source_id_by_name(source_name):
    global delegation_token
    if (not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
    }
    get_source_url_base = "{ip}:{port}/v3/sources".format(ip=proxy_host, port=proxy_port)
    filter_cond = "?filter={\"name\": \""+source_name+"\"}"
    get_source_url=get_source_url_base+filter_cond
    print(get_source_url)
    try:
        response = requests.request('GET', get_source_url,headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to find the source {source_name}.Exiting..")
            logging.error(f"Failed to find  the source {source_name}.Exiting")
            print(response.text)
            logging.info(response)
            return None
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                return result[0]["id"]
            else:
                print(f"There is no existing source with same name {source_name}.Creating new one.")
                logging.info(f"There is no existing source with same name {source_name}.Creating new one.")
                return None
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def get_table_id_by_name(source_id,table_name):
    global delegation_token
    if (not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
    }
    get_table_url_base = "{ip}:{port}/v3/sources/{source_id}/tables".format(ip=proxy_host, port=proxy_port,source_id=source_id)
    #filter_cond = "?filter={\"table\": \""+table_name.upper()+"\"}"
    filter_cond = filter_cond = "?filter={\"table\": {\"$in\": [\"" + table_name + "\"" + ", " + "\"" + table_name.upper() + "\""+"]}}"
    get_table_url=get_table_url_base+filter_cond
    print(get_table_url)
    try:
        response = requests.request('GET', get_table_url,headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to find the table {table_name}.Exiting..")
            logging.error(f"Failed to find  the table {table_name}.Exiting")
            print(response.text)
            logging.info(response)
            return None
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                return result[0]["id"]
            else:
                print(f"There is no existing table with same name {table_name}.Creating new one.")
                logging.info(f"There is no existing table with same name {table_name}.Creating new one.")
                return None
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def get_table_name_by_id(source_id,table_id):
    global delegation_token
    if (not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
    }
    get_table_url_base = "{ip}:{port}/v3/sources/{source_id}/tables/{table_id}".format(ip=proxy_host, port=proxy_port,source_id=source_id,table_id=table_id)
    try:
        response = requests.request('GET', get_table_url_base,headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to find the table {table_id}.Exiting..")
            logging.error(f"Failed to find  the table {table_id}.Exiting")
            print(response.text)
            logging.info(response)
            return None
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                return result["original_table_name"]
            else:
                print(f"There is no existing table with same name {table_id}.Creating new one.")
                logging.info(f"There is no existing table with same name {table_id}.Creating new one.")
                return None
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def set_advance_config(key,value,source_id):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    print("\n\nConfiguring table with below advance configurations")
    logging.info("Configuring table with below advance configurations")
    print(f"{key}:{str(value)}")
    logging.info(f"{key}:{str(value)}")
    #check if advance config already present if present update the value else add the config

    advance_config_body={}
    advance_config_body["key"]=key
    advance_config_body["value"]=str(value)
    advance_config_body["is_active"]=True
    advance_config_payload=json.dumps(advance_config_body)
    request_type="POST"
    #get_table_advance_configuration_url="{host}:{port}/v3/sources/{source_id}/tables/{table_id}/configurations/advance/{key}".format(host=proxy_host, port=proxy_port,source_id=source_id,table_id=table_id,key=advance_config_body["key"])

    source_advance_configuration_url = "{host}:{port}/v3/sources/{source_id}/configurations/advance".format(host=proxy_host, port=proxy_port,source_id=source_id)
    advance_configs_already_present=[]
    try:
        response = requests.request('GET', source_advance_configuration_url, headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to find source advance configs ")
            logging.error(f"Failed to find source advance configs")
            print(response.text)
            logging.info(response)
            #exit(-1)
            #print(response.text)
        else:
            result = response.json().get('result',[])
            advance_configs_already_present.extend([i['key'] for i in result])
            print("advance_configs_already_present",advance_configs_already_present)
            logging.info(f"advance_configs_already_present for source {source_id} are {advance_configs_already_present}")
            if key in advance_configs_already_present:
                source_advance_configuration_url=source_advance_configuration_url+f"/{key}"
                request_type="PUT"
                print("Updating the existing advance config keys with the same name..")
    except Exception as e:
        traceback.print_exc()
        print(str(e))
    print("Advance config url:",source_advance_configuration_url)
    print(request_type)
    logging.info(f"Advance config url: {source_advance_configuration_url}")

    print("Advance config body:\n",advance_config_payload)
    logging.info(f"Advance config body:\n{json.dumps(advance_config_payload)}")
    try:
        response = requests.request(request_type, source_advance_configuration_url, headers=headers, data=advance_config_payload,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to configure table advance config with {key}:{str(value)}")
            logging.error(f"Failed to configure table advance config with {key}:{str(value)}")
            print(response.text)
            logging.info(response)
            #exit(-1)
            #print(response.text)
        else:
            print(f"Configured table with advance configs successfully!")
            logging.info(f"Configured table with advance configs successfully!")
    except Exception as e:
        traceback.print_exc()
        print(str(e))


def create_source_extension(source_extension_body,source_id):
    global delegation_token
    if (not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
    }
    create_source_extension_url = "{ip}:{port}/v3/admin/source-extensions".format(ip=proxy_host, port=proxy_port)
    source_extension_body.pop("package_name")
    try:
        response = requests.request('POST', create_source_extension_url, data=json.dumps(source_extension_body),
                                    headers=headers, verify=False)
        if response.status_code != 200:
            print_error(f"Failed to create the source extension {source_extension_body['name']}")
            logging.error(f"Failed to create the source extension {source_extension_body['name']}")
            print(response.text)
            logging.info(response)
            sys.exit(-100)
        else:
            result = response.json().get('result', {})
            if len(result) != 0:
                print_success(f"Source extension with name {result['id']} created successfully")
                return result["id"]
            else:
                print_error(f"Failed to create the source extension {source_extension_body['name']}.Pleae do it by yourself from UI.")
                logging.error(f"Failed to create the source extension {source_extension_body['name']}.Pleae do it by yourself from UI")

    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)


def create_csv_source(source_creation_body,create_source_extension_bool):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    create_csv_source_url = "{ip}:{port}/v3/sources".format(ip=proxy_host, port=proxy_port)
    existing_source_id_if_any = get_source_id_by_name(source_creation_body['name'])
    if existing_source_id_if_any:
        #if metacrawl_records_count:
        #    set_advance_config("meta_crawl_row_count", metacrawl_records_count, existing_source_id_if_any)
        print(f"Found existing source with same name.Using the source id of the same {existing_source_id_if_any}")
        logging.info(f"Found existing source with same name.Using the source id of the same {existing_source_id_if_any}")
        return existing_source_id_if_any
    try:
        response = requests.request('POST', create_csv_source_url, data=json.dumps(source_creation_body),headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to create the source {source_creation_body['name']}")
            logging.error(f"Failed to create the source {source_creation_body['name']}")
            print(response.text)
            logging.info(response)
            sys.exit(-100)
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                print_success(f"Source with name {result['id']} created successfully")
                set_advance_config("fill_values_for_extra_columns","true",result["id"])
                set_advance_config("add_default_audit_columns_to_target", "false", result["id"])
                set_advance_config("run_first_job_as_cdc", "true", result["id"])
                set_advance_config("sftp_session_configs", "kex=diffie-hellman-group-exchange-sha256", result["id"])
                set_advance_config("file_picking_strategy", "first", result["id"])
                set_advance_config("should_register_hive_udfs", "true", result["id"])
                source_extension_body = {}
                with open("./conf/source_extension_configuration.json", "r") as f:
                    source_extension_body = json.loads(f.read())
                if create_source_extension_bool:
                        create_source_extension(source_extension_body.copy(),result["id"])
                package_name = source_extension_body["package_name"]
                source_extension_body.pop("package_name")
                function_alias = []
                for item in source_extension_body["transformations"]:
                    function_alias.append(f"{item['alias']}:{package_name}.{item['alias']}")
                udfs_functions = ";".join(function_alias)
                set_advance_config("hive_udfs_to_register", udfs_functions, result["id"])
                return result["id"]
            else:
                print_error(f"Failed to create the source {source_creation_body['name']}")
                logging.error(f"Failed to create the source {source_creation_body['name']}")
                sys.exit(-100)
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def configure_csv_source_connection_details(source_id,connection_body):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    source_connection_config_url = "{ip}:{port}/v3/sources/{source_id}/configurations/connection".format(ip=proxy_host, port=proxy_port,source_id=source_id)
    print(f"source_connection_config_url : {source_connection_config_url}")
    logging.info(f"source_connection_config_url : {source_connection_config_url}")
    print(f"source_connection_body :{connection_body}")
    logging.info(f"source_connection_body : {connection_body}")
    try:
        response = requests.request('PUT', source_connection_config_url, data=json.dumps(connection_body),headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to configure the connection for source {source_id}")
            logging.error(f"Failed to configure the connection for source {source_id}")
            print(response.text)
            logging.info(response)
            sys.exit(-100)
        else:
            print_success(f"Source Connection details configured successfully for {source_id}")
            logging.error(f"Source Connection details configured successfully for {source_id}")
            return
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)


def configure_file_mappings(source_id,file_mappings_body):
    file_mappings_body_copy=file_mappings_body.copy()
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    source_filemappings_url = "{ip}:{port}/v3/sources/{source_id}/tables".format(ip=proxy_host, port=proxy_port,source_id=source_id)
    print(f"source_filemappings_url : {source_filemappings_url}")
    logging.info(f"source_filemappings_url : {source_filemappings_url}")
    try:
        #file_mappings_body_copy["configuration"]["source_file_properties"]["header_rows_count"]=1
        response = requests.request('POST', source_filemappings_url, data=json.dumps(file_mappings_body_copy),headers=headers,verify=False)
        if response.status_code != 200:
            if response.json()['iw_code']=="IW10020" or response.json()['iw_code']=="IW10011":
                print("Skipping creation due to existance of table with same name")
                table_name=file_mappings_body["name"]
                table_id=get_table_id_by_name(source_id,table_name)
                #print("Updating header count to ",prev_header_count)
                #mongodb.tables.update_one({"_id":ObjectId(table_id)},{"$set":{"configuration.source_file_properties.header_rows_count":prev_header_count}})
                return table_id
            print_error(f"Failed to create/update table and configure mappings")
            logging.error(f"Failed to create/update table and configure mappings")
            print(response.text)
            logging.info(response)
        else:
            print_success(f"Successfully created table and Configured file mappings")
            logging.error(f"Successfully created table and Configured file mappings")
            print(response.text)
            result = response.json().get('result', {})
            #mongodb.tables.update_one({"_id": ObjectId(result["id"])}, {"$set": {"configuration.source_file_properties.header_rows_count": prev_header_count}})
            return result["id"]
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)


def trigger_fetch_metadata_for_table(source_id,table_id,table_name):
    default_file_properties={
            "header_rows_count": 1,
            "column_separator": ",",
            "column_enclosed_by": "\"",
            "escape_character": "\\",
            "encoding": "UTF-8"
        }

    fetch_metadata_job_body={
        "job_type": "table_fetch_metadata",
        "table_id": table_id,
        "source_file_properties": configuration_json.get("file_properties",default_file_properties)
    }
    try:
        override_file_properties = table_schema_df.query(f"TABLE_NAME == '{table_name}'").fillna('')['FILE_PROPERTIES'].tolist()[0]
        fetch_metadata_job_body["source_file_properties"] = json.loads(override_file_properties)
    except (KeyError, IndexError) as e:
        traceback.print_exc()
        print(str(e))
        print(
            f"Did not find the FILE_PROPERTIES for {table_name} in table schema csv.Going with default FILE_PROPERTIES.")
        logging.info(
            f"Did not find the FILE_PROPERTIES for {table_name} in table schema csv.Going with default FILE_PROPERTIES")
    fetch_metadata_job_body["source_file_properties"]["header_rows_count"]=prev_header_count
    print(fetch_metadata_job_body)
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    fetch_metadata_job_url = "{ip}:{port}/v3/sources/{source_id}/jobs".format(ip=proxy_host, port=proxy_port,source_id=source_id)
    print(f"Fetch metadata job url : {fetch_metadata_job_url}")
    logging.info(f"Fetch metadata job url : {fetch_metadata_job_url}")
    try:
        response = requests.request('POST', fetch_metadata_job_url, data=json.dumps(fetch_metadata_job_body),headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to launch metadata crawl job for {source_id} table {table_id}")
            logging.error(f"Failed to launch metadata crawl job for {source_id} table {table_id}")
            print(response.text)
            logging.info(response)
            return False
        else:
            print_success(f"Successfully crawled schema")
            logging.error(f"Successfully crawled schema")
            return True
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)


def get_interactive_job_id(source_id,table_id):
    if table_id is None:
        return None
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    interactive_job_url_base = "{ip}:{port}/v3/sources/{source_id}/interactive-jobs".format(ip=proxy_host, port=proxy_port,source_id=source_id)
    filter_cond = "?filter={\"table_id\": \"" + table_id + "\"}&sort_by=createdAt&order_by=desc"
    interactive_job_url = interactive_job_url_base+filter_cond
    print(f"Interactive job url : {interactive_job_url}")
    logging.info(f"Interactive  job url : {interactive_job_url}")
    try:
        response = requests.request('GET', interactive_job_url,headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to poll interactive metadata job for {source_id} table {table_id}")
            logging.error(f"Failed to poll interactive metadata job for {source_id} table {table_id}")
            print(response.text)
            logging.info(response)
            return None
        else:
            print_success(f"Successfully got interactive job status")
            logging.error(f"Successfully got interactive job status")
            return (response.json()["result"][0]["id"],response.json()["result"][0]["status"],response.json()["result"][0]["message"],table_id)
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)


def get_interactive_job_status(source_id,table_id,job_id):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    interactive_job_url_base = "{ip}:{port}/v3/sources/{source_id}/interactive-jobs".format(ip=proxy_host, port=proxy_port,source_id=source_id)
    filter_cond = "?filter={\"_id\": \"" + job_id + "\"}"
    interactive_job_url = interactive_job_url_base+filter_cond
    print(f"Interactive job url : {interactive_job_url}")
    logging.info(f"Interactive  job url : {interactive_job_url}")
    try:
        response = requests.request('GET', interactive_job_url,headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to poll interactive metadata job for {job_id}")
            logging.error(f"Failed to poll interactive metadata job for {job_id}")
            print(response.text)
            logging.info(response)
            return None
        else:
            print_success(f"Successfully got interactive job status")
            logging.error(f"Successfully got interactive job status")
            return (response.json()["result"][0]["id"],response.json()["result"][0]["status"],response.json()["result"][0]["message"],table_id)
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def get_tables_list_in_source(source_id):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    get_tables_list_url = "{ip}:{port}/v3/sources/{source_id}/tables".format(ip=proxy_host, port=proxy_port,source_id=source_id)
    tables=[]
    print(f"Table list url : {get_tables_list_url}")
    logging.info(f"Table list url : {get_tables_list_url}")
    try:
        response = requests.request('GET', get_tables_list_url,headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to get tables under {source_id}")
            logging.error(f"Failed to get tables under {source_id}")
            print(response.text)
            logging.info(response)
            return None
        else:
            result = response.json().get("result", [])
            while (len(result) > 0):
                tables.extend(result)
                nextUrl = '{ip}:{port}{next}'.format(next=response.json().get('links')['next'],
                                                                  ip=proxy_host,
                                                                  port=proxy_port)
                response = callurl(nextUrl)
                result = response.json().get("result", [])
            print_success(f"Successfully got tables under {source_id}")
            logging.error(f"Successfully got tables under {source_id}")
            #result=response.json().get("result",[])
            return tables
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def get_table_information(source_id,table_id):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    get_table_info_url = "{ip}:{port}/v3/sources/{source_id}/tables/{table_id}".format(ip=proxy_host, port=proxy_port,source_id=source_id,table_id=table_id)
    print(f"Table Information URL : {get_table_info_url}")
    logging.info(f"Table Information URL : {get_table_info_url}")
    try:
        response = requests.request('GET', get_table_info_url,headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to get tables information for {table_id}")
            logging.error(f"Failed to get tables information for {table_id}")
            print(response.text)
            logging.info(response)
            return []
        else:
            print_success(f"Successfully got tables information for {table_id}")
            logging.error(f"Successfully got tables information for {table_id}")
            result=response.json().get("result",[])
            return result

    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def update_table_information(source_id,table_id,table_body_for_column_updation):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    get_table_info_url = "{ip}:{port}/v3/sources/{source_id}/tables/{table_id}".format(ip=proxy_host, port=proxy_port,source_id=source_id,table_id=table_id)
    print(f"Table Information URL : {get_table_info_url}")
    logging.info(f"Table Information URL : {get_table_info_url}")
    try:
        response = requests.request('PATCH', get_table_info_url,headers=headers,data=json.dumps(table_body_for_column_updation),verify=False)
        if response.status_code != 200:
            print_error(f"Failed to update tables information for {table_id}")
            logging.error(f"Failed to update tables information for {table_id}")
            print(response.text)
            logging.info(response)
            return []
        else:
            print_success(f"Successfully updated tables information for {table_id}")
            logging.error(f"Successfully updated tables information for {table_id}")
            result=response.json().get("result",[])
            return result

    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def get_table_schema(source_id,table_id):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    get_table_schema_url = "{ip}:{port}/v3/sources/{source_id}/tables/{table_id}".format(ip=proxy_host, port=proxy_port,source_id=source_id,table_id=table_id)
    print(f"Table Schema : {get_table_schema_url}")
    logging.info(f"Table Schema : {get_table_schema_url}")
    try:
        response = requests.request('GET', get_table_schema_url,headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to get tables schema for {table_id}")
            logging.error(f"Failed to get tables schema for {table_id}")
            print(response.text)
            logging.info(response)
            return []
        else:
            print_success(f"Successfully got tables schema {table_id}")
            logging.error(f"Successfully got tables schema {table_id}")
            result=response.json().get("result",{}).get("columns",[])
            return result

    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def update_table_schema(source_id,table_id,original_columns,sf_column_names,rename_mappings,columns,table_name):
    global delegation_token
    invalid_columns=configuration_json.get("ignore_columns_from_sf",[])
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    update_table_schema_url = "{ip}:{port}/v3/sources/{source_id}/tables/{table_id}/configurations/schema".format(ip=proxy_host, port=proxy_port,source_id=source_id,table_id=table_id)
    print(f"Table Schema URL : {update_table_schema_url}")
    logging.info(f"Table Schema URL : {update_table_schema_url}")
    payload_data={}
    payload_data["type"]= "column"
    sf_column_names = [column for column in sf_column_names if column not in invalid_columns]
    original_columns=[column for column in original_columns if column not in invalid_columns]
    columns=[column for column in columns if not column["name"].lower().startswith("ziw")]
    if len(original_columns) == len(sf_column_names):
        print("Number of columns in table and in LRF schema provided in csv matched. Updating Table schema with LRF Schema")
        logging.info("Number of columns in table and in LRF schema provided in csv matched. Updating Table schema with LRF Schema")
    else:
        print_error(f"Skipping table {table_name} schema updation since the number of columns doesn't match between csv and csv schema\ncsv_schema in parameter file:{original_columns}\ntable_schema_in_csv:{sf_column_names}")
        logging.error(f"Skipping table {table_name} schema updation since the number of columns doesn't match between csv and csv schema in parameter file\ncsv_schema:{original_columns}\ntable_schema_in_csv:{sf_column_names}")
        skipped_tables.append((table_name,f"Mismatched number of columns between lrf and schema file",original_columns,sf_column_names))
        return
    columns_list=[]
    export_columns_update=[]
    final_columns=[]


    #print("list(zip(original_columns,sf_column_names,columns)",list(zip(original_columns,sf_column_names,columns)))
    for column in list(zip(original_columns,sf_column_names,columns)):
        temp={}
        temp["name"]=rename_mappings.get(column[1].lower(),column[1])
        final_columns.append(temp["name"])
        temp["original_name"]=column[0]
        temp["target_sql_type"]=column[2]["target_sql_type"]
        if column[2].get("format",""):
            temp["format"] = column[2].get("format","")
        export_columns_update.append({"name":column[1],"origName":column[1],"sqlType":column[2]["target_sql_type"]})
        columns_list.append(temp)

    payload_data["columns"]=columns_list
    try:
        response = requests.request('PATCH', update_table_schema_url,data=json.dumps(payload_data),headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to update tables schema for {table_id}")
            logging.error(f"Failed to update tables schema for {table_id}")
            print(response.text)
            logging.info(response)
            return
        else:
            #mongodb.tables.update_one({"_id":ObjectId(table_id)},{"$set":{"export_configuration.columns":export_columns_update}})
            print_success(f"Successfully updated tables schema for {table_id} with column names {final_columns}")
            logging.info(f"Successfully got tables schema for {table_id} with column names {final_columns}")
            print(response.text)
            logging.info(response.text)
            return

    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)


def parallelized_file_mapping_configuration(table,interactive_jobs_list,source_id,file_count_under_table,source_name):
    table_name = table.split("/")[-1]
    sftp_base_path=configuration_json.get("sftp_details",{}).get("sftp_base_path","")
    #num = os.popen(f"databricks fs ls dbfs:{dbfs_base_path}/{subject_matter_folder}/{table}/ | wc -l").read().strip("\n")
    size = 0
    num=0
    file_count_under_table[f"{table_name}"] = num
    #file_size_under_table[f"{table_name}"] = size / 1000
    file_mappings_body = {}
    file_mappings_body["configuration"] = {}
    default_file_properties = {
        "column_enclosed_by": "\"",
        "column_separator": ",",
        "encoding": "UTF-8",
        "escape_character": "\\",
        "header_rows_count": 1
    }
    file_mappings_body["configuration"]["source_file_properties"] = configuration_json.get("file_properties",default_file_properties)
    try:
        override_file_properties = table_schema_df.query(f"TABLE_NAME == '{table_name}'").fillna('')['FILE_PROPERTIES'].tolist()[0]
        file_mappings_body["configuration"]["source_file_properties"] = json.loads(override_file_properties)
    except (KeyError, IndexError) as e:
        traceback.print_exc()
        print(str(e))
        print(
            f"Did not find the FILE_PROPERTIES for {table_name} in table schema csv.Going with default FILE_PROPERTIES.")
        logging.info(
            f"Did not find the FILE_PROPERTIES for {table_name} in table schema csv.Going with default FILE_PROPERTIES")
    file_mappings_body["configuration"]["target_relative_path"] = f"/{table_name}_schema"
    file_mappings_body["configuration"]["deltaLake_table_name"] = f"{table_name}"
    file_mappings_body["configuration"]["source_file_type"] = "csv"
    file_mappings_body["configuration"]["ingest_subdirectories"] = False
    table_path="/{table}"
    try:
        table_path=table_schema_df.query(f"TABLE_NAME == '{table_name}'").fillna('')['LRF path'].tolist()[0]
        if '.' not in table_path.split("/")[-1]:
            table_path=table_path+"/" if not table_path.endswith("/") else table_path
        else:
            dir_name,file_name_ext = os.path.split(table_path)
            table_path=dir_name
    except (KeyError, IndexError) as e:
        traceback.print_exc()
        print(str(e))
        print(
            f"Did not find the LRF path for {table_name} in table schema csv.Going with default path.")
        logging.info(
            f"Did not find the LRF path for {table_name} in table schema csv.Going with default path.")
    file_mappings_body["configuration"]["source_relative_path"] = table_path
    file_mappings_body["configuration"]["exclude_filename_regex"] = "(.*done_bkp.*|.*done_backup.*|.*_bkp)"
    file_mappings_body["configuration"]["include_filename_regex"] = ".*" + table_name.lower() + "(\.[a-z]*)*" + "\.\d+.*"
    file_mappings_body["configuration"]["is_archive_enabled"] = False
    table_schema = ""
    default_schema=configuration_json.get("sfSchema","PUBLIC")
    try:
        table_schema=table_schema_df.query(f"TABLE_NAME == '{table_name}'").fillna('')['DB Name'].tolist()[0]
        table_schema=table_schema.replace("EDW","SDW_ECDW_").replace("DB","_DB")
    except (KeyError, IndexError) as e:
        traceback.print_exc()
        print(str(e))
        print(
            f"Did not find the DB Name for {table_name} in table schema csv.Going with default schema from configuration.json.")
        logging.info(
            f"Did not find the DB Name for {table_name} in table schema csv.Going with default schema from configuration.json.")
    file_mappings_body["configuration"]["target_schema_name"] = table_schema if table_schema and table_schema.strip().lower()!="na" else default_schema
    file_mappings_body["configuration"]["target_table_name"] = f"{table_name}"
    file_mappings_body["configuration"]["target_database_name"] = configuration_json.get("sfDatabase","CSV_AUTOMATED")
    file_mappings_body["configuration"]["is_table_case_sensitive"]=False
    file_mappings_body["configuration"]["is_schema_case_sensitive"] = False
    file_mappings_body["configuration"]["is_database_case_sensitive"] = False
    file_mappings_body["name"] = f"{table_name}"
    file_mappings_body["source"] = source_id
    # print(file_count_under_table)
    table_id = configure_file_mappings(source_id, file_mappings_body)
    print(source_id, table_id)
    metadata_crawl_status = trigger_fetch_metadata_for_table(source_id, table_id,table_name)
    #time.sleep(10)
    #if metadata_crawl_status:
    interactive_job_id = get_interactive_job_id(source_id, table_id)
    interactive_jobs_list.append(interactive_job_id)
    return f"{table_name} File Mappings done"
    #print(interactive_jobs_list)

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False
def is_int(value):
    try:
        int(value)
        return True
    except ValueError:
        return False

def main():
    global refresh_token
    global delegation_token
    parser = argparse.ArgumentParser('CSV Source automation')
    parser.add_argument('--source_name', required=True, help='Pass the name of CSV source to be created')
    parser.add_argument('--refresh_token',type=str,required=True,help='Pass the refresh token of user with admin privileges')
    parser.add_argument('--environment_name', required=False,default="", help='Pass the name of the environment that source should use')
    parser.add_argument('--environment_storage_name', required=False,default="", help='Pass the name of the environment storage that source should use')
    parser.add_argument('--environment_compute_name', required=False,default="", help='Pass the name of the environment compute that source should use')
    parser.add_argument('--create_source_extension', required=False,default="False", help='Pass True to create the source extension',choices=["True","False"])
    log_file_path=f"{cwd}/logs"
    if not os.path.exists(log_file_path):
        os.makedirs(f"{cwd}/logs")
    logging.basicConfig(filename =f'{cwd}/logs/csv_tables_bulk_configuration.log',level = logging.INFO,format = '%(levelname)s:%(asctime)s:%(message)s')
    args = vars(parser.parse_args())
    refresh_token=args.get("refresh_token")
    source_name=args.get("source_name")
    environment_name=args.get("environment_name","")
    environment_storage_name=args.get("environment_storage_name","")
    environment_compute_name=args.get("environment_compute_name","")
    table_schema_path="./table_schema.csv"
    global table_schema_df
    table_schema_df=pd.read_csv(table_schema_path)
    create_source_extension_bool=eval(args.get("create_source_extension","True"))
    #metacrawl_records_count=args.get("meta_crawl_records_count")
    if not environment_name:
        environment_name=configuration_json["environment_name"]
        if environment_name:
            print(f"picking default environment name from configurations.json {environment_name}")
        else:
            print("Could not find the environment_name in configurations.json Please verify the same. Exiting...")
            sys.exit(-100)
    if not environment_storage_name:
        environment_storage_name=configuration_json["environment_storage_name"]
        if environment_storage_name:
            print(f"picking default environment storage name from configurations.json {environment_storage_name}")
        else:
            print("Could not find the environment_storage_name in configurations.json. Please verify the same. Exiting...")
            sys.exit(-100)

    if not environment_compute_name:
        environment_compute_name=configuration_json["environment_compute_name"]
        if environment_compute_name:
            print(f"picking default environment compute name from configurations.json {environment_compute_name}")
        else:
            print("Could not find the environment_compute_name in configurations.json. Please verify the same. Exiting...")
            sys.exit(-100)


    print("environment_name : ",environment_name)
    print("environment_compute_name : ",environment_compute_name)
    print("environment_storage_name : ",environment_storage_name)
    environment_id = get_environment_id_from_name(environment_name)
    environment_storage_id = get_environment_storage_id_from_name(environment_storage_name,environment_id)
    environment_compute_id = get_environment_compute_id_from_name(environment_compute_name, environment_id)
    print(environment_id)
    print(environment_storage_id)
    print(environment_compute_id)
    if configuration_json.get("source_extensions_to_add")!=[]:
        source_extension=configuration_json.get("source_extensions_to_add", ["att_source_extensions"])[0]
    else:
        source_extension="att_source_extensions"
    #prepare the source creation body
    source_creation_body={}
    source_creation_body["name"]=source_name
    source_creation_body["environment_id"]=environment_id
    source_creation_body["storage_id"]=environment_storage_id
    source_creation_body["data_lake_path"]=f"/iw/sources/{source_name}"
    source_creation_body["data_lake_schema"]=configuration_json.get("sfSchema","PUBLIC")
    source_creation_body["staging_schema_name"] = configuration_json.get("sfStageSchema", "PUBLIC")
    source_creation_body["target_database_name"]=configuration_json.get("sfDatabase","CSV_AUTOMATED")
    source_creation_body["is_database_case_sensitive"]= False
    source_creation_body["is_schema_case_sensitive"]= False
    source_creation_body["type"]= "file"
    source_creation_body["sub_type"]= "structured"
    source_creation_body["transformation_extensions"] = [source_extension]
    source_creation_body["is_source_ingested"] = True
    print("source creation body: ",source_creation_body)


    source_id = create_csv_source(source_creation_body,create_source_extension_bool)
    print(f"source id : ",source_id)
    #prepare the source connection body
    sftp_base_path = configuration_json.get("sftp_details",{}).get("sftp_base_path",'')
    if not sftp_base_path.startswith("/"):
        sftp_base_path = "/"+sftp_base_path
    if sftp_base_path!="/":
        sftp_base_path = sftp_base_path.rstrip("/")
    if not sftp_base_path:
        print_error("Please ensure to provide the sftp_base_path config in configurations.json. Exiting...")
        logging.info("Please ensure to provide the sftp_base_path config in configurations.json. Exiting...")
    source_connection_details_body={}
    source_connection_details_body["source_base_path_relative"]= sftp_base_path
    source_connection_details_body["source_base_path"]= ""
    sftp_username=configuration_json.get("sftp_details",{}).get("sftp_username",'')
    sftp_pem_file_path = configuration_json.get("sftp_details",{}).get("sftp_private_key_file_path", '')
    sftp_host = configuration_json.get("sftp_details",{}).get("sftp_host", '')
    sftp_port = int(configuration_json.get("sftp_details",{}).get("sftp_port", 22))
    source_connection_details_body["storage"]={
        "storage_type": "remote",
        "auth_type": "private_key",
        "sftp_host": sftp_host,
        "sftp_port": sftp_port,
        "username": sftp_username,
        "credential": {
            "type": "path",
            "private_key_path": sftp_pem_file_path
        }
    }
    default_warehouse=get_environment_default_warehouse(environment_id)
    source_connection_details_body["warehouse"]=configuration_json.get("sfWarehouse",default_warehouse)

    configure_csv_source_connection_details(source_id,source_connection_details_body)
    folders_from_schema = table_schema_df['TABLE_NAME'].to_list()
    print(f"Trying to create tables under {sftp_base_path}")
    logging.info(f"Trying to create tables under {sftp_base_path}")
    top_level_folder = []
    interactive_jobs_list=[]
    file_count_under_table={}
    print("Picking only the tables/folders in table schema and ignoring other tables which are not present")
    top_level_folder=folders_from_schema
    top_level_folder_len = len(top_level_folder)
    with ThreadPoolExecutor(max_workers=10) as executor:
        for result in executor.map(parallelized_file_mapping_configuration,top_level_folder,[interactive_jobs_list]*top_level_folder_len,[source_id]*top_level_folder_len,[file_count_under_table]*top_level_folder_len,[source_name]*top_level_folder_len):
            if (not token_valid()):
                refresh_delegation_token()
            print(result)
        executor.shutdown(wait=True)
    failed_table_ids=[]
    while(interactive_jobs_list):
        updated_job_list=[]
        for i in interactive_jobs_list:
                #prepare csv if job is successful or failed,as well as pop job out
            job_id,job_status,message,table_id = get_interactive_job_status(source_id,i[3],i[0])
            table_name = get_table_name_by_id(source_id,table_id)
            if job_status=="running":
                updated_job_list.append(i)
            elif job_status=="completed":
                pass
            elif(job_status=="failed"):
                failed_table_ids.append({"table_id":table_id,"table_name":table_name,"job_id":job_id,"job_status":job_status,"error":message})
        interactive_jobs_list=updated_job_list
        if interactive_jobs_list==[]:
            break
        time.sleep(120)
        #    time.sleep(60)

    print("Failed jobs:",failed_table_ids)
    logging.error(f"Failed jobs:{failed_table_ids}")
    failed_tables=[i["table_id"] for i in failed_table_ids]
    with open("./failed_tables.csv","w"):
        pass
    if failed_table_ids:
        print(f"Please fix failed tables before proceeding {failed_table_ids}")
        logging.info(f"Please fix failed tables before proceeding {failed_table_ids}")
        failed_tables_df=pd.DataFrame(failed_table_ids,columns=["table_id","table_name","job_id","job_status","error"])
        failed_tables_df.to_csv("./failed_tables.csv")
        #sys.exit(-100)
    # get the table configurations
    tables_list = get_tables_list_in_source(source_id)
    final_metadata_list=[]
    teradata_spark_mappings={}
    with open("./teradata_spark_dateformat_mappings.json") as teradata_date_map_file:
        teradata_spark_mappings=json.load(teradata_date_map_file)
    #update table schema using table Schema CSV
    for table in tables_list:
        table_id = table["id"]
        if table_id in failed_tables:
            continue
        table_name=table["original_table_name"].lower()
        if table.get("columns",[]) ==[]:
            print_error("Please ensure interactive cluster is up and running and then rerun the script.Exiting..")
            logging.error("Please ensure interactive cluster is up and running and then rerun the script.Exiting..")
            sys.exit(-100)
        try:
            original_column_names = [column["original_name"] for column in table.get("columns") if
                                     not column["original_name"].lower().startswith("ziw")]
            sf_column_names = table_schema_df.query(f"TABLE_NAME == '{table_name}'").fillna('')['LRF_SCHEMA'].tolist()
            rename_mappings=table_schema_df.query(f"TABLE_NAME == '{table_name}'").fillna('')['TABLE_SCHEMA_MAPPINGS'].to_dict()
            rename_mappings=list(rename_mappings.values())[0]
            rename_mappings=json.loads(rename_mappings)
            rename_mappings = {k.lower(): v for k, v in rename_mappings.items()}
            sf_column_names = sf_column_names[0].split(",")
            print(sf_column_names)
            inverse_rename_mappings=dict((v.lower(),k) for k,v in rename_mappings.items())
            if configuration_json.get("source_extensions_to_add")!=[]:
                source_extension_name=configuration_json.get("source_extensions_to_add",["att_source_extensions"])[0]
            else:
                source_extension_name="att_source_extensions"
            #print("\n\ninverse_mappings: ",inverse_rename_mappings)
            if sf_column_names != []:
                update_table_schema(source_id, table_id, original_column_names, sf_column_names,rename_mappings,table.get("columns"),table_name)
                table_info = get_table_information(source_id,table_id)
                table_cols=table_info["columns"]
                function_mappings=table_schema_df.query(f"TABLE_NAME == '{table_name}'").fillna('')['FUNCTION_MAPPINGS'].to_dict()
                function_mappings = list(function_mappings.values())[0]
                function_mappings = json.loads(function_mappings)
                for col in table_cols:
                    #print("column:",col)
                    col["original_name"]=col["name"]

                    lrf_col_name=function_mappings.get(inverse_rename_mappings.get(col["name"].lower(), ""), function_mappings.get(inverse_rename_mappings.get(col["name"].lower(), "").upper(),""))

                    if function_mappings.get(col["name"],lrf_col_name).startswith("to_timestamp(") or function_mappings.get(col["name"],lrf_col_name).startswith("to_date("):
                        if col["target_sql_type"]==93 and not col["name"].startswith("ziw"):
                            if function_mappings.get(col["name"],lrf_col_name).startswith("to_date("):
                                if col["target_sql_type"]!=91:
                                    col["target_sql_type"]=91
                                    if function_mappings.get(col["name"],lrf_col_name)!=lrf_col_name:
                                        format_val=function_mappings.get(col["name"],lrf_col_name).replace("to_date(","").replace(")","").strip().lower()
                                        col["format"]=teradata_spark_mappings.get(format_val,"yyyy-MM-dd")
                            elif function_mappings.get(col["name"],lrf_col_name).startswith("to_timestamp("):
                                format_val=function_mappings.get(col["name"],lrf_col_name).replace("to_timestamp(","").replace(")","").strip().lower()
                                #print("format_val",format_val)
                                col["format"]=teradata_spark_mappings.get(format_val,"yyyy-MM-dd HH:mm:ss")
                        if col["target_sql_type"]==91 and not col["name"].startswith("ziw"):
                            if function_mappings.get(col["name"],lrf_col_name).startswith("to_date("):
                                format_val=function_mappings.get(col["name"],lrf_col_name).replace("to_date(","").replace(")","").strip().lower()
                                col["format"]=teradata_spark_mappings.get(format_val,"yyyy-MM-dd")
                        if col["target_sql_type"]==12 and not col["name"].startswith("ziw"):
                            if function_mappings.get(col["name"],lrf_col_name)=="trim":
                                col["transformation_extension_name"]=source_extension_name
                                col["transformation_function_alias"]="trim_spaces"
                        if function_mappings.get(col["name"], lrf_col_name).startswith("to_timestamp("):
                            col["target_sql_type"] = 93
                            format_val = function_mappings.get(col["name"], lrf_col_name).replace("to_timestamp(", "").replace(")",
                                                                                                                     "").strip().lower()
                            #print("format_val", format_val)
                            fall_back_format=col.get("format","") if col.get("format","") else "yyyy-MM-dd HH:mm:ss"
                            col["format"] = teradata_spark_mappings.get(format_val, fall_back_format)
                        if function_mappings.get(col["name"], lrf_col_name).startswith("to_date("):
                            col["target_sql_type"] = 91
                            format_val = function_mappings.get(col["name"], lrf_col_name).replace("to_date(", "").replace(")","").strip().lower()
                            fall_back_format = col.get("format", "") if col.get("format", "") else "yyyy-MM-dd"
                            col["format"] = teradata_spark_mappings.get(format_val, fall_back_format)
                    elif function_mappings.get(col["name"],lrf_col_name).strip().lower().startswith("custom"):
                        col["transformation_extension_name"] = source_extension_name
                        function_alias = function_mappings.get(col["name"],lrf_col_name)
                        function_alias = re.sub("(C|c)(U|u)(S|s)(T|t)(O|o)(M|m)","",function_alias)
                        col["transformation_function_alias"] = function_alias
                        col["params_details"] = []

                    elif col["target_sql_type"]==4:
                        col["target_sql_type"] = -5

                    else:
                        function_name = function_mappings.get(col["name"],lrf_col_name).strip().replace("(", "$").replace(")", "$").replace(",", "$,")
                        col_name = re.search(":.+?(?=(,|\s|\)))", function_name)
                        if col_name:
                            col_name = col_name.group()
                        else:
                            col_name = ''
                        function_components = function_name.split("$")
                        #print(function_components)
                        function_components = [i for i in function_components if
                                               i != '' and i != col_name]
                        #print(function_components)
                        hiveUDF = ''
                        parameters = []
                        for item in function_components:
                            if not item.strip().startswith(",") and not item.strip().startswith(":"):
                                hiveUDF = hiveUDF + item.capitalize()
                            else:
                                item = item.replace(",", "")
                                if not item.startswith(":"):
                                    parameters.append(item.strip())
                        if hiveUDF:
                            hiveUDF = hiveUDF + "HiveUDF"
                        if hiveUDF.lower().startswith("trim"):
                            col["target_sql_type"]=12
                        param_details = []
                        for parameter in parameters:
                            param_details.append({"value": parameter})
                        if hiveUDF:
                            col["transformation_extension_name"] = source_extension_name
                            col["transformation_function_alias"] = hiveUDF
                            col["params_details"] = param_details
                    #if col["target_sql_type"]==-5 and function_mappings.get(col["name"],lrf_col_name) not in ["decimal_cleanup"]:
                    #    col["target_sql_type"]=12


                # if update_date_present.split(",")[0].lower() == 'yes':
                #     print("Adding updt_dt column to the table if not exists..")
                #     logging.info("Adding updt_dt column to the table if not exists..")
                #     update_column = load_date_present.split(",")[1]
                #     if update_column not in [i["name"] for i in table_cols]:
                #         table_cols.append({"column_type":"target","sql_type" : 93,"is_deleted" : False,"name" : update_column,"original_name" : update_column,"target_sql_type" : 93,"is_audit_column" : False,"transformation_extension_name" : source_extension_name,"transformation_function_alias" : "load_updt_timestamp","format":"yyyy-MM-dd HH:mm:ss.SSS"})
                # if load_date_present.split(",")[0].lower() == 'yes':
                #     print("Adding load_dt column to the table if not exists..")
                #     logging.info("Adding load_dt column to the table if not exists..")
                #     load_column=load_date_present.split(",")[1]
                #     if load_column not in [i["name"] for i in table_cols]:
                #         table_cols.append({"column_type":"target","sql_type": 93, "is_deleted": False, "name": load_column, "original_name": load_column,"target_sql_type": 93, "is_audit_column": False,"transformation_extension_name" : source_extension_name,"transformation_function_alias" : "load_updt_timestamp","format":"yyyy-MM-dd HH:mm:ss.SSS"})
                # #mongodb.update_one("tables",{"_id":ObjectId(table_id)},{"$set":{"columns":table_cols}})

                #Any additional column to be added
                print("Adding additional columns to the table if not exists..")
                logging.info("Adding additional columns to the table if not exists..")
                new_columns_to_add = \
                table_schema_df.query(f"TABLE_NAME == '{table_name}'").fillna('')['NEW_COLUMNS'].tolist()[0]
                if new_columns_to_add:
                    new_columns_to_add=json.loads(new_columns_to_add)
                    for column_data in new_columns_to_add:
                        for k,v in column_data.items():
                            column_type=12
                            if v.lower().startswith('lit'):
                                if  v.lower().strip() == "lit(current_timestamp(0))" or v.lower().strip() == "lit(current_date)":
                                    table_cols.append({"column_type": "target", "sql_type": 93, "is_deleted": False,
                                                       "name": k, "original_name": k,
                                                       "target_sql_type": 93, "is_audit_column": False,
                                                       "transformation_extension_name": source_extension_name,
                                                       "transformation_function_alias": "TimestampTransformerHiveUDF",
                                                       "params_details": [],
                                                       "format": "yyyy-MM-dd HH:mm:ss.SSS"})
                                else:
                                    column_props={"column_type": "target", "sql_type": column_type, "is_deleted": False, "name": k,
                                     "original_name": k, "target_sql_type": column_type, "is_audit_column": False,
                                     "transformation_extension_name": source_extension_name,
                                     "transformation_function_alias": "LitHiveUDF"}
                                    column_val=v.replace("lit(","").replace("LIT(","").rstrip(")").replace("'","")
                                    if is_int(column_val):
                                        column_props["sql_type"] = 4
                                        column_props["target_sql_type"] = 4
                                        column_props["transformation_function_alias"]="NvlIntegerHiveUDF"
                                    elif is_float(column_val):
                                        #precision,scale = str(column_val).split(".")
                                        column_props["sql_type"] = 7
                                        column_props["target_sql_type"] = 7
                                        column_props["transformation_function_alias"] = "NvlFloatHiveUDF"
                                    elif column_val.lower().replace(" ","").replace("'","")=="||@load_batch_id||":
                                        column_props["sql_type"] = 3
                                        column_props["transformation_function_alias"] = "NvlDoubleHiveUDF"
                                        column_props["target_sql_type"] = 3
                                        column_props["target_precision"] = "38"
                                        column_props["target_scale"] = "0"
                                        column_val = "1234567890"
                                    else:
                                        column_props["sql_type"] = 12
                                        column_props["target_sql_type"]=12
                                        column_props["transformation_function_alias"] = "NvlStringHiveUDF"
                                        column_val="\""+column_val+"\""
                                    column_props["params_details"] = [{"value":column_val}]
                                    table_cols.append(column_props)
                            else:
                                function_name=v.strip().replace("(","$").replace(")","$").replace(",","$,")
                                col_name = re.search(":.+?(?=(,|\s|\)))", function_name)
                                if col_name:
                                    col_name=col_name.group()
                                else:
                                    col_name=''
                                function_components = function_name.split("$")
                                function_components = [i for i in function_components if
                                                       i != '' and i != col_name]
                                #print(function_components)
                                hiveUDF = ''
                                parameters = []
                                for item in function_components:
                                    if not item.strip().startswith(",") and not item.strip().startswith(":"):
                                        hiveUDF = hiveUDF + item.capitalize()
                                    else:
                                        item = item.replace(",", "")
                                        if not item.startswith(":"):
                                            parameters.append(item.strip())
                                hiveUDF = hiveUDF + "HiveUDF"
                                param_details = []
                                for parameter in parameters:
                                    param_details.append({"value": parameter})
                                table_cols.append(
                                    {"column_type": "target", "sql_type": 12, "is_deleted": False, "name": k,
                                     "original_name": k, "target_sql_type": 12, "is_audit_column": False,
                                     "transformation_extension_name": source_extension_name,
                                     "transformation_function_alias": hiveUDF,
                                     "params_details": param_details
                                     })


                table_body_for_column_updation={
                    "source":source_id,
                    "name":table_info["name"],
                    "columns":table_cols
                }
                print("table_body_for_column_updation",table_body_for_column_updation)
                update_table_information(source_id,table_id,table_body_for_column_updation)
            else:
                print(f"Skipping the table schema updation for table {table_name} since equivalent entry was not found in table_schema csv")
                logging.info(f"Skipping the table schema updation for table {table_name} since equivalent entry was not found in table_schema csv")
        except (KeyError, IndexError) as e:
            traceback.print_exc()
            print_error(str(e))
            print(f"Skipping the table schema updation for table {table_name} since equivalent entry was not found in sf_schema csv")
            logging.info(f"Skipping the table schema updation for table {table_name} since equivalent entry was not found in sf_schema csv")

    tables_list = get_tables_list_in_source(source_id)
    #print(tables_list)
    #get configuration of each table and dump it as csv in tables_metadata.csv file
    for table in tables_list:
        if table["id"] in failed_tables:
            continue
        table_object={}
        table_object["IWX_TABLE_ID"]=table["id"]
        table_object["IWX_SOURCE_ID"]=table["source"]
        table_object["TABLE_NAME"]=table["name"]
        table_object["DATALAKE_SCHEMA"]=table["data_lake_schema"]
        table_object["DATALAKE_PATH"]=table["data_lake_path"]
        #table_object["FILES_COUNT"]=file_count_under_table[table["original_table_name"]]
        table_object["TARGET_RELATIVE_PATH"]=table["configuration"]["target_relative_path"]
        table_object["DATALAKE_TABLE_NAME"]=table["configuration"]["deltaLake_table_name"]
        table_object["SOURCE_FILE_TYPE"]=table["configuration"]["source_file_type"]
        table_object["INGEST_SUBDIRECTORIES"]=table["configuration"]["ingest_subdirectories"]
        table_object["SOURCE_RELATIVE_PATH"]=table["configuration"]["source_relative_path"]
        table_object["EXCLUDE_FILENAME_REGEX"]=table["configuration"]["exclude_filename_regex"]
        table_object["INCLUDE_FILENAME_REGEX"]=table["configuration"]["include_filename_regex"]
        table_object["IS_ARCHIVE_ENABLED"]=table["configuration"]["is_archive_enabled"]
        table_object["COLUMN_ENCLOSED_BY"]=table["configuration"]["source_file_properties"]["column_enclosed_by"]
        table_object["COLUMN_SEPARATOR"] = table["configuration"]["source_file_properties"]["column_separator"]
        table_object["ENCODING"] = table["configuration"]["source_file_properties"]["encoding"]
        table_object["ESCAPE_CHARACTER"] = table["configuration"]["source_file_properties"]["escape_character"]
        table_object["HEADER_ROWS_COUNT"] = table["configuration"]["source_file_properties"]["header_rows_count"]
        columns=table["columns"]
        column_names=[column["original_name"] for column in columns]
        natural_key_regex_from_config = configuration_json["natural_key_regex"]
        probable_natural_keys=[column["name"] for column in columns \
                               if (column["name"].lower().endswith("id") or \
                                   column["name"].lower().endswith("key") or \
                                   bool(re.match(natural_key_regex_from_config,column["name"])))]
        probable_natural_keys = ','.join(probable_natural_keys)
        table_object["PROBABLE_NATURAL_KEYS"]=probable_natural_keys
        print(f"probable_natural_keys for {table['name']}: {probable_natural_keys}")
        merge_columns=[column for column in column_names if column in configuration_json["merge_water_marks_columns"]]
        append_columns=[column for column in column_names if column in configuration_json["append_water_marks_columns"]]
        table_object["STORAGE_FORMAT"]=configuration_json.get("ingestion_storage_format",table["configuration"]["storage_format"])
        sync_type="FULL_REFRESH"
        watermark_column=None
        if merge_columns and len(probable_natural_keys.split(","))!=0:
            sync_type="INCREMENTAL_MERGE"
            watermark_column=merge_columns[0]
        elif append_columns and len(probable_natural_keys.split(","))!=0:
            sync_type="INCREMENTAL_APPEND"
            watermark_column =append_columns[0]
        table_object["INGESTION_STRATEGY"]=sync_type
        if watermark_column:
            table_object["WATERMARK_COLUMN"]=watermark_column
        table_object["WATERMARK_COLUMN"] = watermark_column
        table_object["SCD_TYPE_2"] = 'FALSE'
        table_object["TARGET_SCHEMA_NAME"]=table["configuration"]["target_schema_name"]
        table_object["TARGET_TABLE_NAME"] = table["configuration"]["target_table_name"]
        table_object["TABLE_GROUP_NAME"] = ''
        table_object["PARTITION_COLUMN"]=''
        table_object["DERIVED_PARTITION"]='FALSE'
        table_object["DERIVED_FORMAT"]=''
        table_object["USER_MANAGED_TABLE"] = 'TRUE'
        final_metadata_list.append(table_object)

    final_df = pd.DataFrame.from_dict(final_metadata_list)
    final_df = final_df[['IWX_TABLE_ID','IWX_SOURCE_ID','TABLE_NAME','DATALAKE_SCHEMA','DATALAKE_PATH' \
        ,'TARGET_RELATIVE_PATH','DATALAKE_TABLE_NAME','SOURCE_FILE_TYPE','INGEST_SUBDIRECTORIES','SOURCE_RELATIVE_PATH' \
        ,'EXCLUDE_FILENAME_REGEX','INCLUDE_FILENAME_REGEX','IS_ARCHIVE_ENABLED','COLUMN_ENCLOSED_BY','COLUMN_SEPARATOR','ENCODING'\
        ,'ESCAPE_CHARACTER','HEADER_ROWS_COUNT','PROBABLE_NATURAL_KEYS' \
        ,'STORAGE_FORMAT','INGESTION_STRATEGY','WATERMARK_COLUMN','SCD_TYPE_2','TARGET_SCHEMA_NAME','TARGET_TABLE_NAME',\
        'TABLE_GROUP_NAME','PARTITION_COLUMN','DERIVED_PARTITION','DERIVED_FORMAT','USER_MANAGED_TABLE']]
    print(f"Find failed tables here:{cwd}/failed_tables.csv")
    #write skipped tables to a file for later debug
    skipped_tables_df=pd.DataFrame(skipped_tables,columns=["table_name","Error","data_file_columns","columns_from_schema"])
    skipped_tables_df.to_csv('./skipped_tables.csv',index=False)
    print(f"Find skipped tables here:{cwd}/skipped_tables.csv")
    print(f"Dumping the dataframe into {cwd}/tables_metadata.csv")
    logging.info(f"Dumping the dataframe into {cwd}/tables_metadata.csv")
    final_df.to_csv(f"{cwd}/tables_metadata.csv",index=False)
    print_success(f"Tables metadata CSV is available here : {cwd}/tables_metadata.csv")
    logging.info(f"Tables metadata CSV is available here : {cwd}/tables_metadata.csv")
    #mongodb.update_one("sources",{"_id":ObjectId(source_id)},{"$set":{"transformation_extensions": configuration_json.get("source_extensions_to_add",["atnt_source_extensions"])}})
    #mongodb.update_one("sources",{"_id":ObjectId(source_id)},{"$set":{"is_source_ingested":True}})



if __name__ == '__main__':
    main()
