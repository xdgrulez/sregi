import logging
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

header_dict = {"Accept": "application/vnd.schemaregistry.v1+json",
               "Content-Type": "application/vnd.schemaregistry.v1+json"}

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get(url_str, auth_tuple):
    logging.debug("GET request: url_str: {}, auth_tuple: {}, headers: {}".format(
        url_str, (auth_tuple[0], "..."), header_dict))
    response = requests_retry_session().get(url_str, auth=auth_tuple, headers=header_dict)
    response_dict = response.json()
    logging.debug("GET response: {}".format(response_dict))
    return response_dict


def post(url_str, auth_tuple, data_dict):
    logging.debug("POST request: url_str: {}, data_dict: {}, auth_tuple: {}, headers: {}".format(
        url_str, data_dict, (auth_tuple[0], "..."), header_dict))
    response = requests_retry_session().post(url_str, json=data_dict, auth=auth_tuple, headers=header_dict)
    response_dict = response.json()
    logging.debug("POST response: {}".format(response_dict))
    return response_dict


def put(url_str, auth_tuple, data_dict):
    logging.debug("PUT request: url_str: {}, data_dict: {}, auth_tuple: {}, headers: {}".format(
        url_str, data_dict, (auth_tuple[0], "..."), header_dict))
    response = requests_retry_session().put(url_str, json=data_dict, auth=auth_tuple, headers=header_dict)
    response_dict = response.json()
    logging.debug("PUT response: {}".format(response_dict))
    return response_dict


def delete(url_str, auth_tuple, hard_bool = True):
    logging.debug("DELETE request: url_str: {}, auth_tuple: {}, headers: {}".format(
        url_str, (auth_tuple[0], "..."), header_dict))
    response = requests_retry_session().delete(url_str, auth=auth_tuple, headers=header_dict)
    response_dict = response.json()
    logging.debug("DELETE response: {}".format(response_dict))
    if hard_bool:
        response = requests_retry_session().delete(url_str + "?permanent=true", auth=auth_tuple, headers=header_dict)
        response_dict = response.json()
        logging.debug("DELETE response (?permanent=true): {}".format(response_dict))
    return response_dict


def get_subjects(base_url_str, auth_tuple):
    url_str = "{}/subjects".format(base_url_str)
    subject_str_list = get(url_str, auth_tuple)
    return subject_str_list


def delete_subject(base_url_str, auth_tuple, subject_str):
    url_str = "{}/subjects/{}".format(base_url_str, subject_str)
    response_dict = delete(url_str, auth_tuple)
    return response_dict


def delete_subject_fix(base_url_str, auth_tuple, subject_str):
    response_dict = delete_subject(base_url_str, auth_tuple, subject_str)
#    print(response_dict)
    return response_dict


def get_versions(base_url_str, auth_tuple, subject_str):
    url_str = "{}/subjects/{}/versions".format(base_url_str, subject_str)
    version_list = get(url_str, auth_tuple)
    return version_list


def get_subject_version(base_url_str, auth_tuple, subject_str, version):
    url_str = "{}/subjects/{}/versions/{}".format(base_url_str, subject_str, version)
    subject_version_dict = get(url_str, auth_tuple)
    return subject_version_dict


def get_latest_subject_version(base_url_str, auth_tuple, subject_str):
    url_str = "{}/subjects/{}/versions/{}".format(base_url_str, subject_str, "latest")
    subject_version_dict = get(url_str, auth_tuple)
    return subject_version_dict


def post_subject_version_import(base_url_str, auth_tuple, subject_str, version_str, id_str, schema_str, schema_type_str):
    url_str = "{}/subjects/{}/versions".format(base_url_str, subject_str)
    if schema_type_str == "":
        subject_version_dict = {"id": id_str, "version": version_str, "schema": schema_str}
    else:
        subject_version_dict = {"id": id_str, "version": version_str, "schema": schema_str, "schemaType": schema_type_str}
    response_dict = post(url_str, auth_tuple, subject_version_dict)
    return response_dict


def post_subject_version(base_url_str, auth_tuple, subject_str, schema_str):
    url_str = "{}/subjects/{}/versions".format(base_url_str, subject_str)
    subject_version_dict = {"schema": schema_str}
    response_dict = post(url_str, auth_tuple, subject_version_dict)
    return response_dict


def post_dummy_subject_version(base_url_str, auth_tuple, subject_str):
    dummy_schema_str = "{\"type\": \"string\"}"
    response_dict = post_subject_version(base_url_str, auth_tuple, subject_str, dummy_schema_str)
    if not ("id" in response_dict and isinstance(response_dict["id"], int)):
        raise Exception("Could not create dummy schema for subject {}.".format(subject_str), response_dict)


def get_subject_config(base_url_str, auth_tuple, subject_str):
    url_str = "{}/config/{}".format(base_url_str, subject_str)
    subject_config_dict = get(url_str, auth_tuple)
    return subject_config_dict


def put_subject_config(base_url_str, auth_tuple, subject_str, subject_config_dict):
    url_str = "{}/config/{}".format(base_url_str, subject_str)
    response_dict = put(url_str, auth_tuple, subject_config_dict)
    return response_dict


def get_global_config(base_url_str, auth_tuple):
    url_str = "{}/config".format(base_url_str)
    global_config_dict = get(url_str, auth_tuple)
    return global_config_dict


def put_global_config(base_url_str, auth_tuple, global_config_dict):
    url_str = "{}/config".format(base_url_str)
    response_dict = put(url_str, auth_tuple, global_config_dict)
    return response_dict


def get_mode(base_url_str, auth_tuple):
    url_str = "{}/mode".format(base_url_str)
    response_dict = get(url_str, auth_tuple)
    return response_dict


def set_mode(base_url_str, auth_tuple, mode_str):
    url_str = "{}/mode".format(base_url_str)
    mode_dict = {"mode": mode_str}
    response_dict = put(url_str, auth_tuple, mode_dict)
    return response_dict
