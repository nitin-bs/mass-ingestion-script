import argparse
import datetime
import json
import logging
import os
import re
import sys
import time
import traceback
from collections import defaultdict
from os.path import exists

import pandas as pd
import requests
import urllib3
from bson import json_util
from colorama import Fore, Style

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#proxy_host = "{}://{}".format(IWUtils.get_value_from_file_or_default("proxy_server_scheme", "http"),IWUtils.get_proxy_host())
#proxy_port = IWUtils.get_proxy_port()
cwd = os.getcwd()

configuration_file= open(f"{cwd}/conf/configurations.json","r")
configuration_json=json.load(configuration_file)
proxy_host=f"{configuration_json.get('protocol','http')}://{configuration_json.get('host','localhost')}"
proxy_port=configuration_json.get('port','3001')
#delegation_token = IWUtils.get_default_delegation_token()
delegation_token=''
workflow_name_id_mappings={}

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

def print_success(message):
    print(Fore.GREEN + message)
    print(Style.RESET_ALL)

def print_error(message):
    print(Fore.RED + message)
    print(Style.RESET_ALL)

def trim_spaces(item):
    return item.strip()

def def_val():
    return []

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

def get_current_user_id():
    global refresh_token
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    users_base="{ip}:{port}/v3/admin/users".format(ip=proxy_host, port=proxy_port)
    user_filter_cond="?filter={\"refreshToken\": \""+refresh_token.replace('+','%2B')+"\"}"
    users_url = users_base+user_filter_cond
    print(user_filter_cond)

    try:
        response = requests.request('GET', users_url, headers=headers,verify=False)
        if response.status_code != 200:
            print(f"Failed to find user details")
            logging.info(f"Failed to find user details")
            print(response.text)
            logging.info(response)
            sys.exit(-100)
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                print_success(f"Found User deatils")
                logging.info(f"Found User deatils")
                return result[0]["id"]
            else:
                print(f"Failed to find user details")
                logging.info(f"Failed to find user details")
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def update_current_user_details(user_id,payload):
    global refresh_token
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    users_url="{ip}:{port}/v3/admin/users/{user_id}".format(ip=proxy_host, port=proxy_port,user_id=user_id)
    print(users_url)

    try:
        response = requests.request('PATCH', users_url, headers=headers,data=json.dumps(payload),verify=False)
        if response.status_code != 200:
            print(f"Failed to find user details")
            logging.info(f"Failed to find user details")
            print(response.text)
            logging.info(response)
            sys.exit(-100)
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                print_success(f"Found User deatils")
                logging.info(f"Found User deatils")
                return result[0]["id"]
            else:
                print(f"Failed to find user details")
                logging.info(f"Failed to find user details")
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def get_domains_accessible_by_user(user_id):
    global refresh_token
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    domains_accessible_by_user_url="{ip}:{port}/v3/admin/users/{user_id}".format(ip=proxy_host, port=proxy_port,user_id=user_id)
    print(domains_accessible_by_user_url)

    try:
        response = requests.request('GET', domains_accessible_by_user_url, headers=headers,verify=False)
        if response.status_code != 200:
            print(f"Failed to find user details")
            logging.info(f"Failed to find user details")
            print(response.text)
            logging.info(response)
            sys.exit(-100)
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                print_success(f"Found User deatils")
                logging.info(f"Found User deatils")
                return result
            else:
                print(f"Failed to find user details")
                logging.info(f"Failed to find user details")
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def get_source_name_from_id(source_id):
    global refresh_token
    global delegation_token
    if (not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
    }
    sources_url = "{ip}:{port}/v3/sources/{source_id}".format(ip=proxy_host, port=proxy_port,source_id=source_id)
    print(sources_url)

    try:
        response = requests.request('GET', sources_url, headers=headers, verify=False)
        if response.status_code != 200:
            print(f"Failed to find source name for {source_id}")
            logging.info(f"Failed to find source name for {source_id}")
            print(response.text)
            logging.info(response)
            sys.exit(-100)
        else:
            result = response.json().get('result', {})
            if len(result) != 0:
                print_success(f"Found Source name for id {source_id}")
                logging.info(f"Found Source name for id {source_id}")
                return result["name"]
            else:
                print(f"Failed to find source name for {source_id}")
                logging.info(f"Failed to find source name for {source_id}")
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)


def is_source_accessible_by_domain(source_id,domain_id):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    domain_accessible_sources_url="{ip}:{port}/v3/domains/{domain_id}/accessible-sources".format(ip=proxy_host, port=proxy_port,domain_id=domain_id)
    print(domain_accessible_sources_url)
    domain_payload={"entity_ids":[source_id]}
    try:
        response = requests.request('GET', domain_accessible_sources_url,headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to find source {source_id} under Domain {domain_id}")
            logging.error(f"Failed to find source {source_id} under Domain {domain_id}")
            print(response.text)
            logging.info(response)
            sys.exit(-100)
        else:
            result = response.json().get('result',[])
            if len(result)!=0:
                source_ids=[source['id'] for source in result]
                if source_id in source_ids:
                    print_success(f"Source {source_id} is accessible by domain")
                    logging.info(f"Source {source_id} is accessible by domain")
                    print(response.text)
                    logging.info(response.text)
                    return True
            else:
                print_error(f"Source {source_id} not accessible by Domain {domain_id}")
                logging.error(f"Source {source_id} not accessible by Domain {domain_id}")
                print(response.text)
                logging.info(response)
                sys.exit(-100)

    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)


def add_source_to_domain(domain_id,source_id):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    add_source_to_domain_url="{ip}:{port}/v3/domains/{domain_id}/accessible-sources".format(ip=proxy_host, port=proxy_port,domain_id=domain_id)
    print(add_source_to_domain_url)
    domain_payload={"entity_ids":[source_id]}
    try:
        response = requests.request('POST', add_source_to_domain_url, data=json.dumps(domain_payload),headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to add source {source_id} to Domain {domain_id}")
            logging.error(f"Failed to add source {source_id} to Domain {domain_id}")
            print(response.text)
            logging.info(response)
            sys.exit(-100)
        else:
            result = response.json()
            if len(result)!=0:
                print_success(f"Added source to domain successfully")
                logging.info(f"Added source to domain successfully")
                print(response.text)
                logging.info(response.text)
                return
            else:
                print_error(f"Failed to add source {source_id} to Domain {domain_id}")
                logging.error(f"Failed to add source {source_id} to Domain {domain_id}")
                print(response.text)
                logging.info(response)
                sys.exit(-100)

    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def create_domain_if_not_exists(domain_name):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    check_domain_exists_base="{ip}:{port}/v3/domains".format(ip=proxy_host, port=proxy_port)
    domain_filter_cond="?filter={\"name\": \""+domain_name+"\"}"
    check_domain_exists_url = check_domain_exists_base+domain_filter_cond
    print(check_domain_exists_url)
    user_id = get_current_user_id()
    try:
        response = requests.request('GET', check_domain_exists_url, headers=headers,verify=False)
        if response.status_code != 200:
            print(f"Failed to find domain {domain_name}. Will try to create a new one")
            logging.info(f"Failed to find domain {domain_name}. Will try to create a new one")
            #print(response.text)
            #logging.info(response)
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                print_success(f"Found Existing Domain with same name {domain_name}. Using its id for further reference")
                logging.info(f"Found Existing Domain with same name {domain_name}. Using its id for further reference")
                accessible_domains_by_user=get_domains_accessible_by_user(user_id)
                domain_list=accessible_domains_by_user["accessible_domains"]
                if result[0]["id"] not in domain_list:
                    domain_list.append(result[0]["id"])
                    #mongodb.update_one("users",{'_id': user_id},{"$set":{"accessible_domains":domain_list}})
                    update_current_user_details(user_id,{"accessible_domains":domain_list})
                return result[0]["id"]
            else:
                print(f"Failed to find domain to name {domain_name}. Will try to create a new one")
                logging.info(f"Failed to find domain to name {domain_name}. Will try to create a new one")
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)


    environment_id = get_environment_id_from_name(configuration_json["environment_name"])
    create_domain_url = "{ip}:{port}/v3/domains".format(ip=proxy_host, port=proxy_port)
    domain_body={"name":domain_name,"environment_ids":[environment_id],"users":[user_id]}
    try:
        response = requests.request('POST', create_domain_url, data=json.dumps(domain_body),headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to create domain {domain_name}")
            logging.error(f"Failed to create domain {domain_name}")
            print(response.text)
            logging.info(response)
            sys.exit(-100)
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                print_success(f"Created domain {domain_name} successfully")
                logging.info(f"Created domain {domain_name} successfully")
                accessible_domains_by_user=get_domains_accessible_by_user(user_id)
                domain_list=accessible_domains_by_user["accessible_domains"]
                #print("result",result)
                if result["id"] not in domain_list:
                    domain_list.append(result["id"])
                    # mongodb.update_one("users",{'_id': user_id},{"$set":{"accessible_domains":domain_list}})
                    update_current_user_details(user_id, {"accessible_domains": domain_list})
                return result["id"]
            else:
                print_error(f"Failed to create domain {domain_name}")
                logging.error(f"Failed to create domain {domain_name}")
                sys.exit(-100)
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)


def get_table_id(tables,table_name):
    for item in tables:
        if(item['table'].lower()==table_name.lower()):
            return str(item['_id'])
    print_error(f"table Not found {table_name}..Exiting..")
    logging.error(f"table Not found {table_name}..Exiting..")
    exit(0)

def get_source_ingestion_mode(source_id):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    get_source_ingestion_mode_url="{host}:{port}/v3/sources/{source_id}/configurations/connection".format(host=proxy_host, port=proxy_port,source_id=source_id)
    try:
        response = requests.request('GET', get_source_ingestion_mode_url, headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to find source {source_id} connection details")
            logging.error(f"Failed to find source {source_id} connection details")
            print(response.text)
            logging.info(response)
            return False
        else:
            result = response.json().get('result',{})
            return result.get('enable_ingestion_via_tpt',False)
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        return False



def make_iwx_compatible_column_name(column,columns):
    if " " not in column:
        return column
    else:
        print(column)
        column=column.replace(" ","")
        r = re.compile(f"{column}\d.*")
        return list(filter(r.match,columns))[0] if len(list(filter(r.match,columns)))!=0 else column

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

def configure_ingestion_strategy(table_id,columns, column_type_dict,\
                                                          table_payload_dict, iwx_column_name_mappings,\
                                                          column_name_case_compatible_dict):
    case_insentive_merge_columns=[column_name_case_compatible_dict.get(i.upper(),'') for i in configuration_json["merge_water_marks_columns"]]
    case_insentive_merge_columns=[i for i in case_insentive_merge_columns if i != '']
    case_insentive_append_columns=[column_name_case_compatible_dict.get(i.upper(),'') for i in configuration_json["append_water_marks_columns"]]
    case_insentive_append_columns=[i for i in case_insentive_append_columns if i != '']
    iwx_compatibe_merge_Columns=[iwx_column_name_mappings[i] for i in case_insentive_merge_columns if i in iwx_column_name_mappings.keys()]
    iwx_compatibe_append_Columns=[iwx_column_name_mappings[i] for i in case_insentive_append_columns if i in iwx_column_name_mappings.keys()]
    merge_Columns=[value for value in iwx_compatibe_merge_Columns if value in columns]
    merge_Columns = [merge_col for merge_col in merge_Columns if column_type_dict[merge_col] in [91, 93, 4]]
    append_Columns=[value for value in iwx_compatibe_append_Columns if value in columns]
    append_Columns = [append_col for append_col in append_Columns if column_type_dict[append_col] in [91, 93, 4]]
    try:
        ingestion_strategy=database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')['INGESTION_STRATEGY'].tolist()[0].strip()
    except (IndexError,KeyError) as e:
        print_error("Did not find the ingestion_strategy in csv file. Defaulting to FULL_REFRESH")
        logging.warning("Did not find the ingestion_strategy in csv file. Defaulting to FULL_REFRESH")
        ingestion_strategy='FULL_REFRESH'
    print("ingestion_strategy: ",ingestion_strategy)
    logging.info(f"ingestion_strategy: {ingestion_strategy}")
    #append_Column=database_info_df.query(f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'")['INGESTION_STRATEGY']
    if ingestion_strategy not in ['INCREMENTAL_APPEND','INCREMENTAL_MERGE','FULL_REFRESH','']:
        print_error("Provided invalid option for INGESTION_STRATEGY.\nShould be one of the following options:\n['INCREMENTAL_APPEND','INCREMENTAL_MERGE','FULL_REFRESH']")
        logging.error("Provided invalid option for INGESTION_STRATEGY.\nShould be one of the following options:\n['INCREMENTAL_APPEND','INCREMENTAL_MERGE','FULL_REFRESH']")
    else:
        if ingestion_strategy == 'INCREMENTAL_MERGE':
            table_payload_dict["sync_type"]= "incremental"
            table_payload_dict["update_strategy"]= "merge"
            scd_type = False
            try:
                scd_type_val=str(database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')['SCD_TYPE_2'].tolist()[0]).strip()
                if eval(scd_type_val.capitalize()) in [False,True]:
                    scd_type=eval(scd_type_val.capitalize())

            except (KeyError,IndexError) as e:
                print_error(f"Did not find the SCD_TYPE_2 column in csv defaulting to SCD TYPE 1 for merge")
                logging.error(f"Did not find the SCD_TYPE_2 column in csv defaulting to SCD TYPE 1 for merge")

            try:
                merge_column=database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')['WATERMARK_COLUMN'].tolist()[0].strip()
                merge_column=column_name_case_compatible_dict[merge_column.upper()]
            except IndexError as e:
                print_error(f"Did not find the watermark columns in CSV even though the strategy is INCREMENTAL_MERGE. Defaulting to first merge Column found: {merge_Columns[0]}")
                logging.error(f"Did not find the watermark columns in CSV even though the strategy is INCREMENTAL_MERGE. Defaulting to first merge Column found: {merge_Columns[0]}")
                if len(merge_column)!=0:
                    merge_column=merge_Columns[0]
                else:
                    print("Defaulting to Full Refresh as watermark_column is not available in csv")
                    logging.info("Defaulting to Full Refresh as watermark_column is not available in csv")
                    table_payload_dict["sync_type"]= "full-load"
            try:
                table_payload_dict["watermark_column"]= iwx_column_name_mappings[merge_column] if iwx_column_name_mappings[merge_column] in columns else merge_Columns[0]
            except IndexError as e:
                print_error(f"Columns in IWX tables are {columns}, but found non existing column {merge_column} for merge")
                logging.error(f"Columns in IWX tables are {columns}, but found non existing column {merge_column} for merge")
                exit(0)
            incremental=True
            sync_type='merge'
            table_payload_dict["is_scd2_table"]=scd_type
            watermark_column=iwx_column_name_mappings[merge_column] if iwx_column_name_mappings[merge_column] in columns else merge_Columns[0]
            print("Configuring table for incremental merge mode")
            logging.info("Configuring table for incremental merge mode")
            print("Setting Merge Mode with watermark Column : ",watermark_column)
            logging.info(f"Setting Merge Mode with watermark Column : {watermark_column}")
        elif ingestion_strategy == 'INCREMENTAL_APPEND':
            table_payload_dict["sync_type"]= "incremental"
            table_payload_dict["update_strategy"]= "append"
            try:
                append_column=database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')['WATERMARK_COLUMN'].tolist()[0].strip()
                append_column = column_name_case_compatible_dict[append_column.upper()]
                append_column=[append_col for append_col in append_column if column_type_dict[append_col] in [91, 93, 4]]
            except IndexError as e:
                print_error(f"Did not find the watermark columns in CSV even though the strategy is INCREMENTAL_APPEND. Defaulting to first append Column found: {append_Columns[0]}")
                logging.error(f"Did not find the watermark columns in CSV even though the strategy is INCREMENTAL_APPEND. Defaulting to first append Column found: {append_Columns[0]}")
                append_column=append_Columns[0]
            table_payload_dict["watermark_column"]= iwx_column_name_mappings[append_column] if iwx_column_name_mappings[append_column] in columns else append_Columns[0]
            incremental=True
            sync_type='append'
            watermark_column=iwx_column_name_mappings[append_column] if iwx_column_name_mappings[append_column] in columns else append_Columns[0]
            print("Configuring table for incremental append mode")
            logging.info("Configuring table for incremental append mode")
            print("Setting Append Mode with watermark Column : ",watermark_column)
            logging.info(f"Setting Append Mode with watermark Column : {watermark_column}")
        else:
            print("Defaulting to Full Refresh")
            logging.info("Defaulting to Full Refresh")
            table_payload_dict["sync_type"]="full-load"
            pass
    return table_payload_dict
def configure_partition_for_table(table_id,columns,column_type_dict,table_payload_dict,iwx_column_name_mappings,column_name_case_compatible_dict):
    global configuration_json
    global database_info_df
    try:
        try:
            partition_column=database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')['PARTITION_COLUMN'].tolist()[0].strip()
            partition_column=column_name_case_compatible_dict[partition_column.upper()]
            partition_column=iwx_column_name_mappings[partition_column]
        except (KeyError,IndexError) as e:
            return table_payload_dict
        if partition_column in columns:
            print(f"Adding parition column {partition_column}..")
            logging.info(f"Adding parition column {partition_column}..")
            if column_type_dict[partition_column] not in [91,93]:
                table_payload_dict["partition_keys"]= [
                    {
                    "partition_column": partition_column,
                    "is_derived_partition": False
                    }]
            else:
                try:
                    derived_partition=str(database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')['DERIVED_PARTITION'].tolist()[0]).strip()
                except (KeyError,IndexError) as e:
                    return table_payload_dict
                if eval(derived_partition.capitalize()) not in [True,False]:
                    print_error(f"derived_partition column in CSV should be either True or False. Unknown property {derived_partition} provided")
                    logging.error(f"derived_partition column in CSV should be either True or False. Unknown property {derived_partition} provided")
                else:
                    if eval(derived_partition):
                        try:
                            derived_format=database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')['DERIVED_FORMAT'].tolist()[0].strip()
                        except (KeyError,IndexError) as e:
                            return table_payload_dict
                        allowed_values_for_date_partition=['dd','MM','yyyy','yyyyMM','MMdd','yyyyMMdd']
                        derive_func_map={}
                        derive_func_map['dd']="day num in month"
                        derive_func_map['MM']="month"
                        derive_func_map['yyyy']="year"
                        derive_func_map['yyyyMM']="year month"
                        derive_func_map['MMdd']="month day"
                        derive_func_map['yyyyMMdd']="year month day"
                        if derived_format not in allowed_values_for_date_partition:
                            print_error(f"derived_format column in CSV should be one of {','.join(allowed_values_for_date_partition)}. Unknown property {derived_format} provided")
                            logging.error(f"derived_format column in CSV should be one of {','.join(allowed_values_for_date_partition)}. Unknown property {derived_format} provided")

                        else:
                            print(f"Deriving the {derive_func_map[derived_format]} from {partition_column} for partition")
                            logging.info(f"Deriving the {derive_func_map[derived_format]} from {partition_column} for partition")
                            table_payload_dict["partition_keys"]= [
                            {
                            "parent_column": partition_column,
                            "derive_format": derived_format,
                            "derive_function": derive_func_map[derived_format],
                            "is_derived_partition": True,
                            "partition_column": "iw_partition_column"
                            }]
                    else:
                        table_payload_dict["partition_keys"]=[
                        {
                        "partition_column": partition_column,
                        "is_derived_partition": False
                        }]
            print(f"Added partion column {partition_column} successfully!")
            logging.info(f"Added partion column {partition_column} successfully!")


        else:
            if partition_column!='':
                print_error(f"{partition_column} column not found in this table. Columns available in table: {','.join(columns)}\n Skipping partition...")
                logging.error(f"{partition_column} column not found in this table. Columns available in table: {','.join(columns)}\n Skipping partition...")
    except Exception as e:
        traceback.print_exc()
        print_error(f"Did not find the partition column in csv file. Skipping partition\n{str(e)}")
        logging.error(f"Did not find the partition column in csv file. Skipping partition\n{str(e)}")

    return table_payload_dict

def get_table_group_id_by_name(tg_name,source_id):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    tg_name_base = "{ip}:{port}/v3/sources/{source_id}/table-groups".format(ip=proxy_host, port=proxy_port,source_id=source_id)
    filter_condition = "?filter={\"name\":\""+tg_name+"\"}"
    tg_name_url = tg_name_base + filter_condition
    try:
        response = requests.request('GET', tg_name_url, headers=headers,verify=False)
        if response.status_code != 200:
            print(f"Failed to find table group with name {tg_name}.Creating new one..")
            logging.info(f"Failed to find table group with name {tg_name}.Creating new one..")
            print(response.text)
            logging.info(response)
            return None
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                return result[0]["id"]
            else:
                print(f"Failed to find table group with name {tg_name}.Creating new one..")
                logging.info(f"Failed to find table group with name {tg_name}.Creating new one..")
                return None
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def configure_table_group(table_group_object,source_id):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    print("\n\nConfiguring table group with below configurations")
    logging.info("Configuring table group with below configurations")
    print(table_group_object)
    logging.info(table_group_object)
    table_group_payload=json.dumps(table_group_object)
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    table_grp_configuration_url = "{host}:{port}/v3/sources/{source_id}/table-groups".format(host=proxy_host, port=proxy_port,source_id=source_id)
    request_type="POST"
    tg_id=get_table_group_id_by_name(table_group_object['name'],source_id)
    if tg_id:
        print(f"Found an existing table group in the source with the same name {table_group_object['name']}, Updating the found table group\n")
        logging.info(f"Found an existing table group in the source with the same name {table_group_object['name']}, Updating the found table group\n")
        request_type="PUT"
        table_grp_configuration_url="{host}:{port}/v3/sources/{source_id}/table-groups/{tg_id}".format(host=proxy_host, port=proxy_port,source_id=source_id,tg_id=tg_id)
    print(table_grp_configuration_url)
    logging.info(table_grp_configuration_url)
    try:
        response = requests.request(request_type, table_grp_configuration_url, headers=headers, data=table_group_payload,verify=False)
        print(response.text)
        #print(table_group_payload)
        if response.status_code != 200:
            print_error(f"Failed to configure table group {table_group_object['name']}")
            logging.error(f"Failed to configure table group {table_group_object['name']}")
            print(response.text)
            logging.info(response.text)
            exit(-1)
            print(response.text)
        else:
            print_success(f"Configured table group {table_group_object['name']} successfully!")
            logging.info(f"Configured table group {table_group_object['name']} successfully!")
    except Exception as e:
        print(str(e))
        logging.warning(str(e))


def configure_table_with_payload(source_id,table_id,table_name,table_payload_dict):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    configure_table_payload_url="{ip}:{port}/v3/sources/{source_id}/tables/{table_id}/configurations/ingestion".format(ip=proxy_host, port=proxy_port,source_id=source_id,table_id=table_id)
    print(configure_table_payload_url)

    try:
        response = requests.request('PUT', configure_table_payload_url,data=json.dumps(table_payload_dict),headers=headers,verify=False)
        if response.status_code != 200:
            print(f"Failed to find configure {table_name}.")
            logging.info(f"Failed to find configure {table_name}.")
            print(response.text)
            logging.info(response.text)
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                print_success(f"Configured {table_name} Successfully")
                logging.info(f"Configured {table_name} Successfully")
                print(response.text)
                logging.info(response.text)
                return
            else:
                print(f"Failed to configure {table_name}")
                logging.info(f"Failed to configure {table_name}")
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)


def tables_configure(source_id,configure_table_group_bool):
    global database_info_df
    global configuration_json
    tables = get_tables_list_in_source(source_id)
    if tables == []:
        print(f"Invalid source ID {source_id}.Please validate the same. Exiting...")
        exit(-100)
    tg_tables = []
    tg_defaults=configuration_json["table_groups"]
    table_group_object={}
    if len(tables)!=0:
        pass
    else:
        print("There should be atleast one table for table creation. script might fail during the table group creation")
        logging.warning("There should be atleast one table for table creation. script might fail during the table group creation")
        exit(-1000)
    tables_should_be_picked_from_config=False
    tg_default_dict=defaultdict(def_val)
    print(tg_default_dict)
    for table in tables:
        tg_tables.append({"table_id":str(table['id'])})
        incremental=False
        sync_type=''
        watermark_column=''
        table_payload_dict={}
        if(not token_valid()):
            refresh_delegation_token()
        table_id = str(table['id'])
        table_name=table['original_table_name']
        columns=[]
        column_type_dict={}
        col_object_array=table.get('columns',[])
        if not col_object_array:
            continue
        probable_natural_keys=[]
        iwx_column_name_mappings={}
        column_name_case_compatible_dict={}
        for col_object in table.get('columns',[]):
            columns.append(col_object['name'])
            column_type_dict[col_object['name']]=col_object['target_sql_type']
            iwx_column_name_mappings[col_object['name']]=col_object['name']
            column_name_case_compatible_dict[col_object['name'].upper()]=col_object['name']
        print("TableName : ",table['original_table_name'])
        logging.info(f"TableName : {table['original_table_name']}")
        print("Columns : ",columns)
        logging.info(f"Columns : {columns}")
        storage_format_user=configuration_json.get("ingestion_storage_format","delta").strip().lower()
        storage_format=configuration_json.get("ingestion_storage_format","delta").strip().lower()
        try:
            storage_format_user = database_info_df.query(f"IWX_TABLE_ID == '{table_id}'").fillna(storage_format_user)['STORAGE_FORMAT'].tolist()[0].strip().lower()
            #print(storage_format_user)
        except IndexError as e:
            print(f"Defaulting to storage format {storage_format_user} as STORAGE_FORMAT column was not found in csv")
            logging.warning(f"Defaulting to storage format {storage_format_user} as STORAGE_FORMAT column was not found in csv")
            storage_format_user=storage_format_user

        if(storage_format_user in ["orc","parquet","avro","delta"]):
            storage_format=storage_format_user
        else:
            print_error("Please provide the valid storage format(orc,parquet,delta or avro)\nExiting..")
            logging.error("Please provide the valid storage format(orc,parquet,delta or avro)\nExiting..")
            exit(0)

        if(storage_format.lower() not in ["orc","parquet","avro","delta"]):
            print_error("Please provide the valid storage format(orc,parquet,delta or avro)\nExiting..")
            logging.error("Please provide the valid storage format(orc,parquet,delta or avro)\nExiting..")
            exit(0)
        else:
            storage_format=storage_format.lower()
        print("storage_format: ",storage_format)
        # configure natural keys
        probable_natural_keys=[]
        try:
            probable_natural_keys = database_info_df.query(f"IWX_TABLE_ID == '{table_id}'")['PROBABLE_NATURAL_KEYS'].tolist()
            logging.info(f"probable_natural_keys:{probable_natural_keys}")
            probable_natural_keys = [x.strip('') for x in probable_natural_keys if type(x) == str]
            logging.info(f"probable_natural_keys:{probable_natural_keys}")
        except (IndexError, KeyError) as e:
            print("No natural keys found. Skipping configuration of natural keys")
            logging.info("No natural keys found. Skipping configuration of natural keys")
            pass

        if probable_natural_keys!=[]:
            probable_natural_keys=list(map(trim_spaces,probable_natural_keys[0].split(',')))

            try:
                probable_natural_keys=[column_name_case_compatible_dict[i.upper()] for i in probable_natural_keys]
                probable_natural_keys=[iwx_column_name_mappings[i] for i in probable_natural_keys]
            except KeyError as e:
                print_error(f"Columns present in this table are : {columns}\n Unknown column provided {str(e)}\n Please validate the same and rerun the script")
                logging.error(f"Columns present in this table are : {columns}\n Unknown column provided {str(e)}\n Please validate the same and rerun the script")
                exit(-100)
            logging.info(f"probable_natural_keys:{probable_natural_keys}")
        else:
            probable_natural_keys=[]
        print("probable_natural_keys: ",probable_natural_keys)
        if len(probable_natural_keys)!=0:
            print("probable_natural_keys found are ",probable_natural_keys)
            logging.info(f"probable_natural_keys found are : {probable_natural_keys}")
            table_payload_dict["natural_keys"]=probable_natural_keys
        table_payload_dict = configure_ingestion_strategy(table_id,columns, column_type_dict,\
                                                          table_payload_dict, iwx_column_name_mappings,\
                                                          column_name_case_compatible_dict)

        print(table_payload_dict)
        if table_payload_dict.get('update_strategy', 'overwrite') in ["merge", "append"]:
            incremental = True
        sync_type = table_payload_dict.get('update_strategy', 'overwrite')
        watermark_column = table_payload_dict.get('watermark_column', '')
        #configure table partition
        table_payload_dict=configure_partition_for_table(table_id,columns,column_type_dict,table_payload_dict,iwx_column_name_mappings,column_name_case_compatible_dict)
        table_payload_dict["storage_format"] = storage_format
        #configure target table schema and target table name
        target_table_name,target_schema_name='',''
        try:
            target_table_name = \
            database_info_df.query(f"IWX_TABLE_ID == '{table_id}'").fillna('')['TARGET_TABLE_NAME'].tolist()[0].strip()
            target_schema_name = \
            database_info_df.query(f"IWX_TABLE_ID == '{table_id}'").fillna('')['TARGET_SCHEMA_NAME'].tolist()[0].strip()
        except (IndexError,KeyError) as e:
            print("Defaulting to original table name and schema name as target table name and schema name as the entry was not found in csv")
            logging.info("Defaulting to original table name and schema name as target table name and schema name as the entry was not found in csv")
            pass

        if target_table_name!= '':
            print("target_table_name : ",target_table_name)
            logging.info(f"target_table_name : {target_table_name}")
            table_payload_dict["target_table_name"]=target_table_name
        if target_schema_name!='':
            print("target_schema_name : ",target_schema_name)
            logging.info(f"target_schema_name : {target_schema_name}")
            table_payload_dict["target_schema_name"]=target_schema_name


        #configure source file properties
        table_payload_dict["source_file_properties"]={}
        try:
            column_enclosed_by = database_info_df.query(f"IWX_TABLE_ID == '{table_id}'").fillna('')['COLUMN_ENCLOSED_BY'].tolist()[0].strip()
            table_payload_dict["source_file_properties"]["column_enclosed_by"] = column_enclosed_by if column_enclosed_by else "\""
        except (KeyError,IndexError) as e:
            print(f"Did not find the table {table_name} in csv, keeping the default COLUMN_ENCLOSED_BY as '\'")
            logging.info(f"Did not find the table {table_name} in csv, keeping the default COLUMN_ENCLOSED_BY as '\'")

        try:
            column_separator = database_info_df.query(f"IWX_TABLE_ID == '{table_id}'").fillna('')['COLUMN_SEPARATOR'].tolist()[0].strip()
            table_payload_dict["source_file_properties"]["column_separator"] = column_separator if column_separator else ","
        except (KeyError,IndexError) as e:
            print(f"Did not find the table {table_name} in csv, keeping the default COLUMN_SEPARATOR as ','")
            logging.info(f"Did not find the table {table_name} in csv, keeping the default COLUMN_SEPARATOR as ','")

        try:
            encoding = database_info_df.query(f"IWX_TABLE_ID == '{table_id}'").fillna('')['ENCODING'].tolist()[0].strip()
            table_payload_dict["source_file_properties"]["encoding"] = encoding if encoding else "UTF-8"
        except (KeyError,IndexError) as e:
            print(f"Did not find the table {table_name} in csv, keeping the default ENCODING as UTF-8")
            logging.info(f"Did not find the table {table_name} in csv, keeping the default ENCODING as UTF-8")

        try:
            escape_character = database_info_df.query(f"IWX_TABLE_ID == '{table_id}'").fillna('')['ESCAPE_CHARACTER'].tolist()[0].strip()
            table_payload_dict["source_file_properties"]["escape_character"] = escape_character if escape_character else "\\"
        except (KeyError, IndexError) as e:
            print(f"Did not find the table {table_name} in csv, keeping the default ESCAPE_CHARACTER as '\\'")
            logging.info(f"Did not find the table {table_name} in csv, keeping the default ESCAPE_CHARACTER as '\\'")
        try:
            header_rows_count = database_info_df.query(f"IWX_TABLE_ID == '{table_id}'").fillna('')['HEADER_ROWS_COUNT'].tolist()[0]
            table_payload_dict["source_file_properties"]["header_rows_count"] = header_rows_count if header_rows_count else 1
        except (KeyError, IndexError) as e:
            print(f"Did not find the table {table_name} in csv, keeping the default HEADER_ROWS_COUNT as 1")
            logging.info(f"Did not find the table {table_name} in csv, keeping the default HEADER_ROWS_COUNT as 1")

        #is user managed table
        user_managed_table=True
        try:
            user_managed_table = str(
                database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')['USER_MANAGED_TABLE'].tolist()[0]).strip()
            if eval(user_managed_table.capitalize()) in [False, True]:
                user_managed_table = eval(user_managed_table.capitalize())

        except (KeyError, IndexError) as e:
            print_error(f"Did not find the USER_MANAGED_TABLE column in csv defaulting to True for merge")
            logging.error(f"Did not find the USER_MANAGED_TABLE column in csv defaulting to True for merge")
        if user_managed_table:
            table_payload_dict["is_table_user_managed"] = True
        else:
            table_payload_dict["is_table_user_managed"] = False

        configure_table_with_payload(source_id, table_id, table_name, table_payload_dict)


        #table group attachment
        self_table_grp_name=''
        try:
            self_table_grp_name=database_info_df.query(f"IWX_TABLE_ID == '{table_id}'").fillna('')['TABLE_GROUP_NAME'].tolist()[0].strip()
        except (IndexError,KeyError) as e:
            pass
        if self_table_grp_name!='':
            tg_default_dict[self_table_grp_name].append(table_id)

    for table_group in tg_defaults:
        tables_from_config = []
        if tg_default_dict[table_group['name']] != []:
            for table in tg_default_dict[table_group['name']]:
                tables_from_config.append({"table_id": table})
            tables_should_be_picked_from_config = True
        elif (len(table_group.get('table_names', [])) != 0):
            tables_from_config = []
            for table in table_group.get('table_names', []):
                print(f"Configuring table group {table_group['name']}")
                tables_from_config.append(
                    {"table_id": get_table_id(tables, table)})
            tables_should_be_picked_from_config = True
            # print(tables_from_config)
        else:
            tables_from_config = []
            for table in tables:
                tables_from_config.append({"table_id": str(table['id'])})
            tables_should_be_picked_from_config = True
        table_group_object["environment_compute_template"] = {
            "environment_compute_template_id": get_environment_compute_id_from_name(
                table_group.get('environment_compute_name', ''), get_environment_id_from_name(configuration_json["environment_name"]))}
        table_group_object["name"] = table_group["name"]
        table_group_object["max_parallel_entities"] = table_group.get('max_parallel_tables', len(tables))
        default_warehouse = get_environment_default_warehouse(get_environment_id_from_name(configuration_json["environment_name"]))
        table_group_object["warehouse"] = configuration_json.get("sfWarehouse", default_warehouse)
        if (tables_should_be_picked_from_config):
            table_group_object["tables"] = tables_from_config
        else:
            table_group_object["tables"] = tg_tables
        if (configure_table_group_bool):
            configure_table_group(table_group_object, source_id)
        else:
            pass

def workflow_with_name_exists(domain_id,workflow_name):
    global delegation_token
    if (not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
    }
    check_workflow_exists_base = "{ip}:{port}/v3/domains/{domain_id}/workflows".format(ip=proxy_host, port=proxy_port,domain_id=domain_id)
    filter_cond = "?filter={\"name\": \"" + workflow_name + "\"}"
    check_workflow_exists_url = check_workflow_exists_base + filter_cond
    print(check_workflow_exists_url)

    try:
        response = requests.request('GET', check_workflow_exists_url, headers=headers, verify=False)
        if response.status_code != 200:
            print(f"Failed to find workflow {workflow_name}. Will try to create a new one")
            logging.info(f"Failed to find workflow {workflow_name}. Will try to create a new one")
            print(response.text)
            logging.info(response)
            return None
        else:
            result = response.json().get('result', {})
            if len(result) != 0:
                print_success(f"Found Existing Workflow with same name {workflow_name}. Using its id for further reference")
                logging.info(f"Found Existing Workflow with same name {workflow_name}. Using its id for further reference")
                return result[0]["id"]
            else:
                print(f"Failed to find workflow to name {workflow_name}. Will try to create a new one")
                logging.info(f"Failed to find workflow to name {workflow_name}. Will try to create a new one")
                return None
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)

def create_workflow(workflow_body_dict,domain_id):
    global delegation_token
    global workflow_name_id_mappings
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    workflow_configuration_url = "{host}:{port}/v3/domains/{domain_id}/workflows".format(host=proxy_host, port=proxy_port,domain_id=domain_id)
    workflow_payload_json = json.dumps(workflow_body_dict,default=json_util.default)
    print(workflow_payload_json)
    try:
        response = requests.request("POST", workflow_configuration_url, headers=headers, data=workflow_payload_json,verify=False)
        if response.status_code != 200:
            print(Fore.RED + f"Failed to create workflow {workflow_body_dict['name']}")
            print(Style.RESET_ALL)
            print(response.text)
            exit(-1)
        else:
            result=response.json().get('result',{})
            print(Fore.GREEN + f"Configured workflow {workflow_body_dict['name']} successfully!")
            workflow_name_id_mappings[result['id']] = workflow_body_dict["name"]
            print(Style.RESET_ALL)
            return result['id']
    except Exception as e:
        print(str(e))

def update_workflow(workflow_id,workflow_body_dict,domain_id):
    global delegation_token
    global workflow_name_id_mappings
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    workflow_updation_url = "{host}:{port}/v3/domains/{domain_id}/workflows/{workflow_id}".format(host=proxy_host, port=proxy_port,domain_id=domain_id,workflow_id=workflow_id)
    workflow_payload_json = json.dumps(workflow_body_dict,default=json_util.default)
    print(workflow_payload_json)
    try:
        response = requests.request("PATCH", workflow_updation_url, headers=headers, data=workflow_payload_json)
        if response.status_code != 200:
            print(Fore.RED + f"Failed to update workflow {workflow_body_dict['name']}")
            print(Style.RESET_ALL)
            print(response.text)
            exit(-1)
        else:
            result=response.json().get('result',{})
            print(Fore.GREEN + f"Updated workflow {workflow_body_dict['name']} successfully!")
            print(Style.RESET_ALL)
            return result['id']
    except Exception as e:
        print(str(e))
def run_workflow(workflow_id,domain_id,poll_workflow_bool):
    global delegation_token
    global workflow_name_id_mappings
    refresh_delegation_token()
    run_id=None
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    workflow_run_url = "{host}:{port}/v3/domains/{domain_id}/workflows/{workflow_id}/start".format(host=proxy_host, port=proxy_port,domain_id=domain_id,workflow_id=workflow_id)
    #print(workflow_run_url)
    try:
        response = requests.request("POST", workflow_run_url, headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to trigger workflow {workflow_name_id_mappings.get(workflow_id,workflow_id)}")
            logging.error(f"Failed to trigger workflow {workflow_name_id_mappings.get(workflow_id,workflow_id)}")
            print(response.text)
            logging.info(response)
            exit(-1)
            #print(response.text)
        else:
            run_id=response.json().get('result',None)['id']
            print_success(f"Triggered workflow {workflow_name_id_mappings.get(workflow_id,workflow_id)} successfully with run Id {run_id}")
            logging.info(f"Triggered workflow {workflow_name_id_mappings.get(workflow_id,workflow_id)} successfully with run Id {run_id}")
    except Exception as e:
        print(str(e))
        exit(-1)




    #monitor the workflow run
    if poll_workflow_bool:
        status='running'
        workflow_status_url="{host}:{port}/v3/admin/workflows/{workflow_id}/runs/{run_id}".format(host=proxy_host, port=proxy_port,domain_id=domain_id,workflow_id=workflow_id,run_id=run_id)
        #print(workflow_status_url)
        while status not in ['success','failed','aborted','cancelled']:
            try:
                if not token_valid():
                    refresh_delegation_token()
                    headers = {
                        'Authorization': f'Bearer {delegation_token}',
                        'Content-Type': 'application/json'
                    }
                response = requests.request("GET", workflow_status_url, headers=headers,verify=False)
                if response.status_code != 200:
                    print_error(f"Failed to fetch workflow {workflow_name_id_mappings.get(workflow_id,workflow_id)} status \n")
                    logging.error(f"Failed to fetch workflow {workflow_name_id_mappings.get(workflow_id,workflow_id)} status \n")
                    print(response.text)
                    logging.info(response.text)
                    exit(-1)
                    #print(response.text)
                else:
                    status = response.json()["result"]["workflow_status"]["state"]
                    print(f"Status of workflow {workflow_name_id_mappings.get(workflow_id,workflow_id)} run {response.json().get('result',{}).get('id',None)} is {status} \n")
                    logging.info(f"Status of workflow {workflow_name_id_mappings.get(workflow_id,workflow_id)} run {response.json().get('result',{}).get('id',None)} is {status} \n")
            except Exception as e:
                print(str(e))
                traceback.print_exc()
                logging.error(e, exc_info=True)
                exit(-1)
            time.sleep(30)

        print(f"Workflow completed with status: {status}")
        logging.info(f"Workflow completed with status: {status}")
    else:
        pass

def get_table_group_names_under_source(source_id,tg_name):
    global delegation_token
    if(not token_valid()):
        refresh_delegation_token()
    headers = {
        'Authorization': f'Bearer {delegation_token}',
        'Content-Type': 'application/json'
        }
    get_table_group_names_under_source_base="{ip}:{port}/v3/sources/{source_id}/table-groups".format(ip=proxy_host, port=proxy_port,source_id=source_id)
    filter_cond = "?filter={\"name\":\""+tg_name+"\"}"
    get_table_group_names_under_source_url = get_table_group_names_under_source_base + filter_cond
    print(get_table_group_names_under_source_url)

    try:
        response = requests.request('GET', get_table_group_names_under_source_url,headers=headers,verify=False)
        if response.status_code != 200:
            print_error(f"Failed to find {tg_name} under {source_id}.")
            logging.error(f"Failed to find {tg_name} under {source_id}.")
            print(response.text)
            logging.info(response.text)
            return []
        else:
            result = response.json().get('result',{})
            if len(result)!=0:
                print_success(f"Found table group {tg_name} Successfully")
                logging.info(f"Found table group {tg_name} Successfully")
                print(response.text)
                logging.info(response.text)
                return result[0]['id']
            else:
                print_error(f"Failed to find {tg_name} under {source_id}.")
                logging.error(f"Failed to find {tg_name} under {source_id}.")
                exit(-100)
    except Exception as e:
        traceback.print_exc()
        print(str(e))
        logging.error(e, exc_info=True)
        sys.exit(-100)


def configure_workflow(source_id,domain_id):
    source_accessible_by_domain=is_source_accessible_by_domain(source_id,domain_id)
    source_name = get_source_name_from_id(source_id)
    if not source_accessible_by_domain:
        print_error(f"Given source {source_name} is not accessible by the domain {domain_id}. Please add the same")
        logging.error(f"Given source {source_name} is not accessible by the domain {domain_id}. Please add the same")
        exit(0)
    now =datetime.datetime.now()
    workflow_model={
        "name": "",
        "description": "",
        "child_workflow_ids":[],
        "workflow_graph": {
            "tasks": [
                {
                    "is_group": False,
                    "title": "Ingest Source",
                    "description": "",
                    "task_id": "SI_KE7I",
                    "task_type": "ingest_table_group",
                    "location": "-385 -105",
                    "run_properties": {
                        "num_retries": 0,
                        "trigger_rule": "all_success"
                    },
                    "task_properties": {
                        "source_id": "",
                        "table_group_id": "",
                        "ingest_type": "all"
                    }
                }
            ],
            "edges": []
        }
    }
    table_groups=configuration_json.get("table_groups",[])
    workflows_list=[]
    for tg in table_groups:
        tg_name=tg.get('name',None)
        if  tg_name is None:
            print_error("Table group section in configurations.json is not found. Please validate the same. Exiting...")
            logging.error("Table group section in configurations.json is not found. Please validate the same. Exiting...")
            exit(0)
        table_group_list=[get_table_group_names_under_source(source_id,tg_name)]
        if len(table_group_list) == 0:
            print_error("No Table groups under given source")
            logging.error("No Table groups under given source")
            exit(-1000)
        for table_group in table_group_list:
            #print(table_group["name"])
            workflow_model["name"]=f"{source_name}_{tg_name}_ingest_export_wf"
            for task in workflow_model["workflow_graph"]["tasks"]:
                task["task_properties"]["source_id"]=source_id
                task["task_properties"]["table_group_id"]=table_group
            print(f"Configure workflow {workflow_model['name']} with :")
            logging.info(f"Configure workflow {workflow_model['name']} with :")
            #print(json.dumps(workflow_model))
            print(workflow_model)
            logging.info(workflow_model)
            workflow_id = workflow_with_name_exists(domain_id,workflow_model["name"])
            if not workflow_id:
                workflow_id = create_workflow(workflow_model,domain_id)
            else:
                update_workflow(workflow_id,workflow_model,domain_id)

            if(workflow_id):
                workflows_list.append(workflow_id)
            else:
                print_error(f"Failed to configure workflow {workflow_model['name']}")

    return workflows_list


def main():
    global refresh_token
    global database_info_df
    global configuration_json
    parser = argparse.ArgumentParser(description='CSV Source Bulk table Configuration')
    #parser.add_argument('--source_id',type=str,required=True,help='source_id for which tables needs to be configured')
    parser.add_argument('--refresh_token',type=str,required=True,help='Pass the refresh token of user with admin privileges')
    parser.add_argument('--configure',type=str,default="all",help='Pass the configure type (all(default),table,tg or workflow)')
    parser.add_argument('--domain_name',type=str,required=False,help='Pass the domain name in which workflows are to be created')
    parser.add_argument('--metadata_csv_path',type=str,required=True,help='Pass the absolute path of metadata csv file')
    parser.add_argument('--config_json_path',type=str,required=True,help='Pass the absolute path of configuration json file')
    logging.basicConfig(filename =f'{cwd}/logs/csv_tables_bulk_configuration.log',level = logging.INFO,format = '%(levelname)s:%(asctime)s:%(message)s')


    args = parser.parse_args()
    domain_name=args.domain_name
    refresh_token=args.refresh_token
    configure_type=args.configure
    metadata_csv_path=args.metadata_csv_path
    config_json_path=args.config_json_path
    if exists(metadata_csv_path):
        database_info_df=pd.read_csv(metadata_csv_path)
    else:
        print_error(f"Did not find the file {metadata_csv_path} for reading")
        logging.error(f"Did not find the file {metadata_csv_path} for reading")
        print("Exiting...")
        exit(-1000)
    source_id = database_info_df["IWX_SOURCE_ID"].iloc[0]
    #configuration_file= open(f"{cwd}/conf/configurations.json","r")
    configuration_file=open(config_json_path,"r")
    configuration_json=json.load(configuration_file)
    domain_id=None
    if args.domain_name:
        domain_id = create_domain_if_not_exists(domain_name)
        add_source_to_domain(domain_id,source_id)

    if(configure_type not in ['all','table','tg','workflow']):
        print_error(f"Unknown configure type {configure_type} \nPlease provide the valid configure type (all,table,tg,workflow)")
        logging.error(f"Unknown configure type {configure_type} \nPlease provide the valid configure type (all,table,tg,workflow)")
        exit(0)
    run_workflow_bool=configuration_json.get('run_workflow',True)
    poll_workflow_bool = configuration_json.get('poll_workflow',False)
    if run_workflow_bool not in [True,False]:
        print_error("Please provide one of the valid values true/false to run_workflow variable in configurations.json\n")
        logging.error("Please provide one of the valid values true/false to run_workflow variable in configurations.json\n")
        exit(0)
    print(database_info_df)
    try:
        if(configure_type=='all'):
            tables_configure(source_id,True,)
            workflow_list = configure_workflow(source_id,domain_id)
            print("workflows created :",workflow_list)
            logging.info("workflows created : {workflow_list}")
            print("Sleeping for 60 seconds for workflow configurations")
            logging.info("Sleeping for 60 seconds for workflow configurations")
            time.sleep(60)
            for workflow_id in workflow_list:
                if workflow_id and run_workflow_bool:
                    run_workflow(workflow_id,domain_id,poll_workflow_bool)
        elif(configure_type=='table' or configure_type=='tg'):
            if(configure_type=='table'):
                tables_configure(source_id,False)
            else:
                tables_configure(source_id,True)
        else:
            workflow_list = configure_workflow(source_id,domain_id)
            print("workflows created / updated : ",workflow_list)
            logging.info("workflows created /updated : {workflow_list}")
            print("Sleeping for 60 seconds for workflow configurations")
            logging.info("Sleeping for 60 seconds for workflow configurations")
            time.sleep(60)
            for workflow_id in workflow_list:
                if workflow_id and run_workflow_bool:
                    run_workflow(workflow_id,domain_id,poll_workflow_bool)
    finally:
        configuration_file.close()
        print(f"\n\n\nLogs for this script is available under {cwd}/logs/csv_tables_bulk_configuration.log")



if __name__ == '__main__':
    main()
