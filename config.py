import os
from configparser import ConfigParser


if os.environ.get('DEV_ENVIRONMENT', 'LOCAL') in ["PRODUCTION", "AZURE_STAGING"]:
    DB_CONFIG = {
        "USERNAME": os.environ["DB_USERNAME"],
        "PASSWORD": os.environ["DB_PASSWORD"],
        "HOST": os.environ["DB_HOST"],
        "NAME": os.environ["DB_NAME"]
    }

    OKTA_CONFIG={
        "CLIENT_ID": os.environ["OKTA_CLIENT_ID"],
        "CLIENT_SECRET": os.environ["OKTA_CLIENT_SECRET"],
        "ISSUER": os.environ["OKTA_ISSUER"],
        "AUDIENCE": os.environ["OKTA_AUDIENCE"]
    }
    PERSONICLE_AUTH_API = {
        "ENDPOINT": os.environ["PERSONICLE_AUTH_API_ENDPOINT"]
    }

    PERSONICLE_SCHEMA_API = {
        "ENDPOINT": os.environ["PERSONICLE__API_ENDPOINT"]
    }

else:
    config_object = ConfigParser()
    config_object.read("config.ini")
    DB_CONFIG = config_object["CREDENTIALS_DATABASE"]
    OKTA_CONFIG = config_object["OKTA"]
    PERSONICLE_AUTH_API = config_object["PERSONICLE_AUTH_SERVICE"]
    PERSONICLE_SCHEMA_API = config_object["PERSONICLE_DATA_DICTIONARY"]
    EVENTHUB_CONFIG=config_object["EVENTHUB"]
    
    # DB_CONFIG = {
    #     "USERNAME" : os.getenv('USERNAME'),
    #     "PASSWORD": os.getenv('PASSWORD'),
    #     "HOST": os.getenv('HOST'),
    #     "NAME": os.getenv('NAME')
    # }

    # PERSONICLE_AUTH_API = {
    #     "ENDPOINT": os.getenv('AUTH_ENDPOINT')
    # }
