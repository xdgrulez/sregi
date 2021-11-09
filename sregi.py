import json
import logging
import os
import yaml
import re
import sr
import sys
import time

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(name)s %(levelname)s %(message)s",
                    datefmt="%d-%b-%y %H:%M:%S")


def get_sr_config(sr_str):
    with open("srs/{}.yaml".format(sr_str), "r") as file:
        sr_dict = yaml.load(file, Loader=yaml.FullLoader)
        base_url_str = sr_dict["url"]
        user_str = sr_dict["user"]
        password_str = sr_dict["password"]
    auth_tuple = (user_str, password_str)
    return base_url_str, auth_tuple


def upload(sr_str):
    if sr_str == "prod" or sr_str == "eu-prod-dedicated" or sr_str == "eu-dev-dedicated":
        logging.error("Emergency brake :)")
        return
    #
    base_url_str, auth_tuple = get_sr_config(sr_str)
    #
    start_time_float = time.time()
    subject_version_counter_int = 0
    #
    logging.info("Ensuring that the Schema Registry is in IMPORT mode...")
    response_dict = sr.get_mode(base_url_str, auth_tuple)
    mode_str = response_dict["mode"]
    if not mode_str == "IMPORT":
        response_dict = sr.set_mode(base_url_str, auth_tuple, "IMPORT")
        if not response_dict == {"mode": "IMPORT"}:
            raise Exception("Could not set Schema Registry to IMPORT mode (current mode: {}).".format(mode_str))
    logging.info("...done.")
    #
    logging.info("Setting the global configuration...")
    with open("srs/{}/global_config.json".format(sr_str), "r") as file:
        global_config_json_str = file.read()
    global_config_dict = json.loads(global_config_json_str)
    compatibility_level_str = global_config_dict["compatibilityLevel"]
    global_config_dict1 = {"compatibility": compatibility_level_str}
    response_dict = sr.put_global_config(base_url_str, auth_tuple, global_config_dict1)
    if not response_dict == global_config_dict1:
        raise Exception("Could not set the global configuration.", response_dict)
    logging.info("...done.")
    #
    subject_str_list = [subject_str for subject_str in os.listdir("srs/{}".format(sr_str))
                        if os.path.isdir("srs/{}/{}".format(sr_str, subject_str))]
    subjects_int = len(subject_str_list)
    i = 0
    for subject_str in subject_str_list:
        i = i + 1
        logging.info("{}/{} Subject {}".format(i, subjects_int, subject_str))
        #
        version_file_str_list = [version_file_str for version_file_str in
                                 os.listdir("srs/{}/{}".format(sr_str, subject_str))
                                 if os.path.isfile("srs/{}/{}/{}".format(sr_str, subject_str, version_file_str)) and
                                 version_file_str.endswith(".json") and
                                 version_file_str.replace(".json", "").isdigit()]
        version_file_str_list.sort(key=lambda s: int(re.match(r"^(\d+).json$", s).group(1)))
        for version_file_str in version_file_str_list:
            with open("srs/{}/{}/{}".format(sr_str, subject_str, version_file_str), "r") as file:
                subject_version_json_str = file.read()
            version_str = version_file_str.replace(".json", "")
            subject_version_dict = json.loads(subject_version_json_str)
            id_str = subject_version_dict["id"]
            schema_str = subject_version_dict["schema"]
            if "schemaType" in subject_version_dict:
                schema_type_str = subject_version_dict["schemaType"]
            else:
                schema_type_str = ""
            logging.info("  Creating subject version {}/{} (id: {})...".format(subject_str, version_str, id_str))
            response_dict = sr.post_subject_version_import(base_url_str, auth_tuple,
                                                           subject_str, version_str, id_str, schema_str, schema_type_str)
            if not response_dict == {"id": int(id_str)}:
                raise Exception("Could not create subject version {}/{} (id: {})".format(
                    subject_str, version_str, id_str), response_dict)
            logging.info("  ...done.")
            subject_version_counter_int = subject_version_counter_int + 1
        if os.path.exists("srs/{}/{}/config.json".format(sr_str, subject_str)):
            with open("srs/{}/{}/config.json".format(sr_str, subject_str), "r") as file:
                subject_config_json_str = file.read()
            logging.info("Creating configuration for subject {}...".format(subject_str))
            subject_config_dict = json.loads(subject_config_json_str)
            subject_compatibility_str = subject_config_dict["compatibilityLevel"]
            subject_config_dict1 = {"compatibility": subject_compatibility_str}
            response_dict = sr.put_subject_config(base_url_str, auth_tuple, subject_str, subject_config_dict1)
            if not response_dict == subject_config_dict1:
                raise Exception("Could not set subject compatibility level to {}.".format(subject_compatibility_str))
            logging.info("...done.")
    #
    logging.info("Re-setting the Schema Registry mode to {} mode...".format(mode_str))
    if not mode_str == "IMPORT":
        response_dict = sr.set_mode(base_url_str, auth_tuple, mode_str)
        if not response_dict == {"mode": mode_str}:
            raise Exception("Could not re-set Schema Registry to {} mode.".format(mode_str))
    logging.info("...done.")
    #
    end_time_float = time.time()
    logging.info("Created {} schemas in {} seconds.".format(subject_version_counter_int,
                                                            end_time_float - start_time_float))


def download(sr_str):
    base_url_str, auth_tuple = get_sr_config(sr_str)
    #
    start_time_float = time.time()
    #
    os.mkdir("srs/{}".format(sr_str))
    #
    logging.info("Getting the global configuration...")
    global_config_dict = sr.get_global_config(base_url_str, auth_tuple)
    global_config_json_str = json.dumps(global_config_dict)
    with open("srs/{}/global_config.json".format(sr_str), "w") as file:
        file.write(global_config_json_str)
    logging.info("...done.")
    #
    logging.info("Getting all subjects...")
    subject_str_list = sr.get_subjects(base_url_str, auth_tuple)
    subjects_int = len(subject_str_list)
    logging.info("...done ({} subjects).".format(subjects_int))
    #
    i = 0
    for subject_str in subject_str_list:
        if "/" in subject_str:
            subject_str = subject_str.replace("/", "%2F")
        os.mkdir("srs/{}/{}".format(sr_str, subject_str))
        #
        i = i + 1
        logging.info("{}/{} Subject {}".format(i, subjects_int, subject_str))
        #
        subject_config_dict = sr.get_subject_config(base_url_str, auth_tuple, subject_str)
#        logging.info(subject_config_dict)
        if "compatibilityLevel" in subject_config_dict:
            subject_config_json_str = json.dumps(subject_config_dict)
            with open("srs/{}/{}/config.json".format(sr_str, subject_str), "w") as file:
                file.write(subject_config_json_str)
        #
        logging.info("  Getting all versions of subject {}...".format(subject_str))
        version_list = sr.get_versions(base_url_str, auth_tuple, subject_str)
        logging.info("  ...done ({}).".format(version_list))
        # Workaround for a bug in CCloud Schema Registry as of May 10, 2020:
        # If a subject has only one imported version >1, it retrieved by its version number...
        # ...using "latest" does the trick.
        latest_int = version_list[-1]
#        if len(version_list) == 1 and version_list[0] > 1:
#            version_list = ["latest"]
        for version in version_list:
            logging.info("    Getting subject version corresponding to subject {} and version {}...".format(
                subject_str, version))
            subject_version_dict = sr.get_subject_version(base_url_str, auth_tuple, subject_str, version)
            subject_version_json_str = json.dumps(subject_version_dict)
            version_int = version
            if version == "latest":
                version_int = latest_int
            with open("srs/{}/{}/{}.json".format(sr_str, subject_str, version_int), "w") as file:
                file.write(subject_version_json_str)
            logging.info("    ...done.")
    #
    end_time_float = time.time()
    logging.info("Got {} subjects in {} seconds.".format(subjects_int, end_time_float - start_time_float))


def delete_all(sr_str):
    if sr_str == "prod" or sr_str == "eu-prod-dedicated" or sr_str == "eu-dev-dedicated":
        logging.error("Emergency brake :)")
        return
    #
    base_url_str, auth_tuple = get_sr_config(sr_str)
    #
    start_time_float = time.time()
    #
    logging.info("Ensuring that the Schema Registry is in IMPORT mode...")
    response_dict = sr.get_mode(base_url_str, auth_tuple)
    mode_str = response_dict["mode"]
    if not mode_str == "READWRITE":
        response_dict = sr.set_mode(base_url_str, auth_tuple, "READWRITE")
        if not response_dict == {"mode": "READWRITE"}:
            raise Exception("Could not set Schema Registry to READWRITE mode (current mode: {}).".format(mode_str))
    logging.info("...done.")
    #
    logging.info("Getting all subjects...")
    subject_str_list = sr.get_subjects(base_url_str, auth_tuple)
    subjects_int = len(subject_str_list)
    logging.info("...done ({} subjects).".format(subjects_int))
    #
    i = 0
    for subject_str in subject_str_list:
        if "/" in subject_str:
            subject_str = subject_str.replace("/", "%2F")
        #
        i = i + 1
        logging.info("{}/{} Deleting subject {}...".format(i, subjects_int, subject_str))
        #
        response_dict = sr.delete_subject_fix(base_url_str, auth_tuple, subject_str)
        if not isinstance(response_dict, list):
            raise Exception("Could not delete subject {}.".format(subject_str),
                            response_dict)
        #
        logging.info("...done.")
    #
    logging.info("Re-setting the Schema Registry to {} mode...".format(mode_str))
    if not mode_str == "READWRITE":
        response_dict = sr.set_mode(base_url_str, auth_tuple, mode_str)
        if not response_dict == {"mode": mode_str}:
            raise Exception("Could not re-set Schema Registry to {} mode.".format(mode_str))
    logging.info("...done.")
    #
    end_time_float = time.time()
    logging.info("Deleted {} subjects in {} seconds.".format(subjects_int, end_time_float - start_time_float))


def main():
    command_str = sys.argv[1]
    sr_str = sys.argv[2]
    if command_str == "download":
        download(sr_str)
    elif command_str == "upload":
        upload(sr_str)
    elif command_str == "deleteall":
        delete_all(sr_str)


main()
