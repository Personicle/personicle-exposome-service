from flask import Flask, Response, jsonify, request
import logging
import os
from fastavro import validate, parse_schema
import json
from data_dictionary import find_datastream, find_event_definition
from config import DATA_DICTIONARY_SERVER_SETTINGS

# Log_Format = "%(levelname)s %(asctime)s - %(message)s"
# logging.basicConfig(filename = "data_dictionary.log",
#                     filemode = "w",
#                     format = Log_Format,
#                     level=logging.DEBUG
#                     )
# logger = logging.getLogger(__name__) 

script_dir = os.path.dirname(__file__)

app = Flask(__name__)

@app.route("/")
def test_application():
    # logger.info("Testing the application")
    return "Testing schema validation application"


@app.route("/validate-data-packet", methods=["POST"])
def validate_data_packet():
    data_type = request.args.get("data_type")
    current_event = request.get_json()
    # current_event = event.body_as_json(encoding='UTF-8')
    try:
        logging.info("Received data packet for data type {}:\n{}".format(data_type, json.dumps(current_event, indent=2)))

        stream_type = current_event.get('streamName', None)
        
        # match the stream name to the data dictionary
        if data_type == "event":
            # stream_information = find_event_definition(stream_type)
            stream_information = {"base_schema": "event_schema.avsc"}
        elif data_type == "datastream":
            stream_information = find_datastream(stream_type)
        else:
            # logger.warn("Incorectly formatted data, data type not found in data dictionary: {}".format(json.dumps(current_event, indent=2)))
            return Response("Incorectly formatted data, data type not found in data dictionary", 403)
        if stream_information is None:
            return Response("stream type not found", 404)

        # stream_information = match_data_dictionary(stream_type)
        # get the corresponding schema and the table
        file_path = os.path.join(script_dir, f"avro_modules/{stream_information['base_schema']}")
        
        with open(file_path, 'r') as fi:
            schema = json.load(fi)
        
        # validate the event with avro schema
        print("Matching with schema: {}".format(schema))
        parsed_schema = parse_schema(schema) 
        try:
            if validate(current_event, parsed_schema, raise_errors=True):
                # logger.info("Valid event")
                return jsonify({"schema_check": True})
            else:
                logging.info("Packet not formatted correctly {}".format(current_event))
                return jsonify({"schema_check": False})
        except Exception as e:
            logging.info("{}".format(current_event))
            logging.error("Exception while validating packet: {}".format(e))
            return jsonify({"schema_check": False})
    except KeyError as e:
        print(e)
        return Response("Incorrect packet format", 422)


@app.route("/match-data-dictionary", methods=["GET","POST"])
def match_data_dictionary():
    data_type = request.args.get("data_type")
    print("data type is {}".format(data_type))
    stream_type = request.args.get("stream_name")
    print("stream name is {}".format(stream_type))
    if data_type == "event":
        stream_information = find_event_definition(stream_type)
    elif data_type == "datastream":
        stream_information = find_datastream(stream_type)
    else:
        # logger.warn("Incorectly formatted data, data type not found in data dictionary: {}".format(stream_type))
        return Response("Incorectly formatted data, data type must be event or datastream", 403)
    if stream_information:
        return jsonify(stream_information)
    else:
        return Response("Stream type not found", 403)


if __name__ == "__main__":
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    app.run(DATA_DICTIONARY_SERVER_SETTINGS['HOST_URL'], port=DATA_DICTIONARY_SERVER_SETTINGS['HOST_PORT'])
    
