import http.client
import json
import os
import logging

from server.http_server import data_storage
from utils.fluent_bit_manager import FluentBitManager
from utils.network import find_available_port

logger = logging.getLogger(__name__)

def create_connection(server, port):
    return http.client.HTTPConnection(server, port)

def create_headers():
    return {
        'Content-Type': 'application/json'
    }

def create_payload(json_filename):
    try:
        file_name = os.path.abspath(os.path.join(os.path.dirname(__file__), './data_files/', json_filename))
        with open(file_name, 'r') as file:
            data = file.read().strip()
            return data
    except FileNotFoundError:
        return json.dumps({"error": "File not found"}, indent=4)
    except json.JSONDecodeError:
        return json.dumps({"error": "Invalid JSON format"}, indent=4)

def send_requests(conn, num_requests, headers, json_payload):
    responses = []
    for i in range(num_requests):
        conn.request("POST", "/", body=json_payload, headers=headers)
        response = conn.getresponse()
        responses.append({
            'status': response.status,
            'reason': response.reason,
            'data': response.read().decode()
        })
    return responses


def test_send_data():
    try:
        service = Service("in_http_config")
        service.start()
        output = service.runtest_send_data('localhost', service.flb_listener_port, 'sample_data.json')
        logger.info(f"response: {output}")
        service.stop()
        assert len(output) > 0

        # Verify response details if necessary
        for response in output:
            assert response['status'] == 201
            assert response['reason'] == 'Created'
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if service.flb.process is not None:
            service.stop()
        raise

class Service:
    def __init__(self, config_file):
        self.config_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '../config/', config_file))
        data_storage['logs'] = []

    def start(self):
        self.flb = FluentBitManager(self.config_file)
        self.flb_listener_port =find_available_port(starting_port=45000)
        os.environ['FLUENT_BIT_TEST_LISTENER_PORT'] = str(self.flb_listener_port)
        logger.info(f"Fluent Bit listener port: {self.flb_listener_port}")
        self.test_suite_http_port = find_available_port(starting_port=50000)
        os.environ['TEST_SUITE_HTTP_PORT'] = str(self.test_suite_http_port)
        logger.info(f"test suite http port: {self.test_suite_http_port}")
        self.flb.start()

    def runtest_send_data(self, server, port, json_filename):
        conn = create_connection(server, port)
        headers = create_headers()
        json_payload = create_payload(json_filename)
        responses = send_requests(conn, 1, headers, json_payload)
        conn.close()
        return responses

    def stop(self):
        self.flb.stop()