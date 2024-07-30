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
import time
import base64
import requests

# OTel imports to convert from JSON to OTLP Protobuf
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from google.protobuf import json_format

# local imports
from utils.data_utils import read_json_file
from utils.network import find_available_port
from utils.fluent_bit_manager import FluentBitManager, ENV_FLB_HTTP_MONITORING_PORT

# Add the directory containing http_server.py to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src/server')))

from server.otlp_server import otlp_server_run, data_storage

logger = logging.getLogger(__name__)

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

class Service:
    def __init__(self, config_file):
        # Compose the absolute path for the Fluent Bit configuration file
        self.config_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '../config/', config_file))
        data_storage['logs'] = []

    def start(self):
        # Create an instance of the Fluent Bit Manager
        self.flb = FluentBitManager(self.config_file)

        # set a listener port (input plugin) for the config file (note: we use an environment variable
        # so Fluent Bit will replace it in the config once it loads it)
        self.flb_listener_port = find_available_port(starting_port=45000)
        os.environ['FLUENT_BIT_TEST_LISTENER_PORT'] = str(self.flb_listener_port)
        logger.info(f"Fluent Bit listener port: {self.flb_listener_port}")

        # find and set a listener port for the local server to receive the
        # Fluent Bit output
        self.test_suite_http_port = find_available_port(starting_port=50000)
        os.environ['TEST_SUITE_HTTP_PORT'] = str(self.test_suite_http_port)
        logger.info(f"test suite http port: {self.test_suite_http_port}")

        # Start an OTLP Server (receiver)
        otlp_server_run(self.test_suite_http_port)

        # Ping the test suite server
        url = f'http://127.0.0.1:{self.test_suite_http_port}/ping'

        # try to ping the server
        while True:
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    break
            except requests.exceptions.ConnectionError:
                time.sleep(0.5)
                logger.info("waiting for server...")


        # Start Fluent Bit
        self.flb.start()

    def read_response(self):
        while len(data_storage['logs']) <= 0:
            time.sleep(0.5)
            logger.info("waiting for response...")

        json_str = json_format.MessageToJson(data_storage['logs'][0])
        logger.info(f"{json_str}")
        return json.loads(json_str)

    def send_request(self, payload, content_type='application/x-protobuf'):
        # Send the protobuf payload
        url = f'http://localhost:{self.flb_listener_port}/v1/logs'
        headers = {'Content-Type': content_type}
        response = requests.post(url, data=payload.SerializeToString(), headers=headers)
        print(f'Status code: {response.status_code}')
        print(f'Response text: {response.text}')

    def send_json_as_otel_protobuf(self, json_input):
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../tests', 'data_files'))
        json_payload_dict = read_json_file(os.path.join(base_path, json_input))
        json_payload_str = json.dumps(json_payload_dict)

        log_request = ExportLogsServiceRequest()
        protobuf_payload = json_format.Parse(json_payload_str, log_request)

        self.send_request(protobuf_payload)

        return self.read_response()

    def stop(self):
        self.flb.stop()
        requests.post(f'http://localhost:{self.test_suite_http_port}/shutdown')


# This is a full pipeline test, the file tests_logs_001.in.json, represents an OpenTelemetry Log payload in
# JSON format which gets converted to Protobuf.
#
# Then Fluent Bit is started having an OpenTelemetry listener (input plugin) and OpenTelemetry output plugin
# that sends the data back to the test suite. Note that we instance a dummy OTLP server in this test suite
# so we can check the data that is being sent back.
def test_opentelemetry_to_opentelemetry_basic_log():
    service = Service("001-fluent-bit.yaml")
    service.start()
    output = service.send_json_as_otel_protobuf("test_logs_001.in.json")
    logger.info(f"response: {output}")
    service.stop()

    assert len(output['resourceLogs']) == 2


# Start a Fluent Bit Pipeline with Dummy message and then it gets handle by OpenTelemetry output, the config
# aims to populate traceId and spanId fields with the values from the Dummy message.
#
# issue : https://github.com/fluent/fluent-bit/issues/9071
# fixed : https://github.com/fluent/fluent-bit/pull/9074
def test_dummy_to_opentelemetry_log():
    service = Service("002-fluent-bit.yaml")
    service.start()
    output = service.read_response()
    logger.info(f"response: {output}")
    service.stop()

    # direct reference to the record
    record = output['resourceLogs'][0]['scopeLogs'][0]['logRecords'][0]

    # notes on traceid and spanid: the test case encodes the values as hex strings, Fluent Bit OpenTelemetry
    # output plugin will decode and pack them as bytes. When the data is sent back to the test suite, the values
    # are encoded as base64 strings (Python thing). So we need to decode them back to bytes and compare them.
    assert base64.b64decode(record['traceId']) == bytes.fromhex('63560bd4d8de74fae7d1e4160f2ee099')
    assert base64.b64decode(record['spanId'])  == bytes.fromhex('251484295a9df731')
