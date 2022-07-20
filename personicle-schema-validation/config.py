import os
import pathlib
import configparser

__file_path = os.path.abspath(__file__)
__dir_path = os.path.dirname(__file_path)

PROJ_LOC=pathlib.Path(__dir_path)
AVRO_SCHEMA_LOC=os.path.join(PROJ_LOC, "avro_modules")

# Database url format
# dialect+driver://username:password@host:port/database
# postgresql+pg8000://dbuser:kx%25jj5%2Fg@pghost10/appdb

if int(os.environ.get("INGESTION_PROD", '0')) != 1:
    print("in the dev environment")
    print("environment variables: {}".format(list(os.environ.keys())))
    SQLITE_DATABASE_LOCATION=os.path.join(PROJ_LOC, "database")
    SQLITE_DATABASE_NAME="user_access_tokens.db"

    __app_config = configparser.ConfigParser()
    __app_config.read(os.path.join(PROJ_LOC,'config.ini'))
    print(PROJ_LOC,list(__app_config.keys()))

    DATA_DICTIONARY_SERVER_SETTINGS = __app_config['DATA_DICTIONARY_SERVER']
else:
    print("in the prod environment")
    DATA_DICTIONARY_SERVER_SETTINGS = {
        'HOST_URL': os.environ.get('DATA_DICTIONARY_HOST', "0.0.0.0"),
        'HOST_PORT': os.environ.get('DATA_DICTIONARY_PORT', 5002)
    }
