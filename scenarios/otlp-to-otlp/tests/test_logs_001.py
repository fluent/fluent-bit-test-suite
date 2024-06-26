#  Fluent Bit
#  ==========
#  Copyright (C) 2015-2024 The Fluent Bit Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import sys
import json
import logging
import requests
import time
import threading

# OTel imports to convert from JSON to OTLP Protobuf
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from google.protobuf import json_format

# local imports
from utils.data_utils import read_json_file
from utils.network import find_available_port
from utils.fluent_bit_manager import FluentBitManager, ENV_FLB_HTTP_MONITORING_PORT

# Add the directory containing http_server.py to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src/server')))

from otlp_server import otlp_server_run, data_storage

logger = logging.getLogger(__name__)

def send_request(port, payload):
    # Send the protobuf payload
    url = f'http://localhost:{port}/v1/logs'
    headers = {'Content-Type': 'application/x-protobuf'}
    response = requests.post(url, data=payload.SerializeToString(), headers=headers)
    print(f'Status code: {response.status_code}')
    print(f'Response text: {response.text}')


#  [Fluent Bit Test Suite]
#    - Start Fluent Bit:
#      - a custom configuration file
#      - set 3 environment variables:
#        - FLUENT_BIT_HTTP_MONITORING_PORT: port where Fluent Bit will expose internal metrics
#        - FLUENT_BIT_TEST_LISTENER_PORT: port used by the config file to define where to listen
#          for incoming connections
#        - TEST_SUITE_HTTP_PORT: local port on this suite which is used by Fluent Bit to send the
#          data back
#
#   [Test Suite] --> writes a request --> [Fluent Bit] --> forwards the request --> [Test Suite]
#
#    In the process above, Fluent Bit decode the request, process it and encode it back.

def run_unit_test(file_data_in, config_file):
    # this thing needs to be refactored through a proper API
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../tests', 'data_files'))

    # Load the JSON payload and convert it to protobuf
    json_payload_dict = read_json_file(os.path.join(base_path, file_data_in))
    json_payload_str = json.dumps(json_payload_dict)

    log_request = ExportLogsServiceRequest()
    protobuf_payload = json_format.Parse(json_payload_str, log_request)

    # Compose the absolute path for the Fluent Bit configuration file
    config_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '../config/', config_file))
    #logger.info(f"config file {config_file}")


    # Create an instance of the Fluent Bit Manager
    flb = FluentBitManager(config_file)

    # set a listener port (input plugin) for the config file (note: we use an environment variable
    # so Fluent Bit will replace it in the config once it loads it)
    flb_listener_port = find_available_port(starting_port=45000)
    os.environ['FLUENT_BIT_TEST_LISTENER_PORT'] = str(flb_listener_port)
    logger.info(f"Fluent Bit listener port: {flb_listener_port}")

    # find and set a listener port for the local server to receive the
    # Fluent Bit output
    test_suite_http_port = find_available_port(starting_port=50000)
    os.environ['TEST_SUITE_HTTP_PORT'] = str(test_suite_http_port)
    logger.info(f"test suite http port: {test_suite_http_port}")

    # # Start an OTLP Server (receiver)
    flask_thread = threading.Thread(target=otlp_server_run, kwargs={"port": int(test_suite_http_port)})
    flask_thread.daemon = True
    flask_thread.start()

    # Start Fluent Bit
    flb.start()

    send_request(flb_listener_port, protobuf_payload)

    # a dummy wait
    while len(data_storage['logs']) <= 0:
        time.sleep(0.5)
        logger.info("waiting for response...")

    json_str = json_format.MessageToJson(data_storage['logs'][0])

    # stop Fluent Bit
    flb.stop()

    return json.loads(json_str)

def test_basic_log():
    output = run_unit_test("test_logs_001.in.json", "fluent-bit.yaml")
    logger.info(f"test: {output}")
    assert len(output['resourceLogs']) == 2

