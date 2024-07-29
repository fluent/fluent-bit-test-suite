import http.client, time, json, os, ssl, logging, requests

from utils.fluent_bit_manager import FluentBitManager
from utils.network import find_available_port
from server.otlp_server import  data_storage

logger = logging.getLogger(__name__)

# Definition of constant url elastic
# PORT_FAKE_ELASTIC = 9200
def create_connection(server, port):
    return http.client.HTTPConnection(server, port)


def create_headers():
    return {
        'Content-Type': 'application/json'
    }
"""
Load json file to generate the payload
"""
def create_payload(json_filename):
    try:
        file_name = os.path.abspath(os.path.join(os.path.dirname(__file__), './data_files/', json_filename))
        with open(file_name, 'r') as file:
            # Read the entire file as text and strip unnecessary whitespace
            data = file.read().strip()
            return data
    except FileNotFoundError:
        return json.dumps({"error": "File not found"}, indent=4)
    except json.JSONDecodeError:
        return json.dumps({"error": "Invalid JSON format"}, indent=4)
"""
Generic send request
"""
def send_requests(conn, num_requests, headers, json_payload):
    responses = []
    for i in range(num_requests):
        conn.request("POST", "/_bulk", body=json_payload, headers=headers)
        response = conn.getresponse()
        responses.append({
            'status': response.status,
            'reason': response.reason,
            'data': response.read().decode()
        })
    return responses

"""
CREATE INDEX
"""
def test_create_index():
    try:
        service = Service("elastic_ingest")
        service.start()
        output = service.runtest_create_index('localhost', service.flb_listener_port,'create_index.json')
        logger.info(f"response: {output}")
        service.stop()
        assert len(output) == 1

        # Verify response details
        for response in output:
            assert response['status'] == 200
            assert response['reason'] == 'OK'
            assert response['data'] == '{"errors":false,"items":[{"index":{"status":201,"result":"created"}}]}'
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        service.stop()
        raise

"""
CREATE MULTIPLOE DOCUMENTS
"""
def test_create_multiple_documents():
    try:
        service = Service("elastic_ingest")
        service.start()
        output = service.runtest_create_multiple_documents('localhost', service.flb_listener_port,'create_multiple_documents.json')
        logger.info(f"response: {output}")
        service.stop()
        assert len(output) == 1

        # Verify response details
        for response in output:
            assert response['status'] == 200
            assert response['reason'] == 'OK'
            assert response['data'] == '{"errors":false,"items":[{"index":{"status":201,"result":"created"}},{"index":{"status":201,"result":"created"}},{"index":{"status":201,"result":"created"}},{"index":{"status":201,"result":"created"}}]}'
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        service.stop()
        raise

"""
UOPDATE MULTIPLE DOCUMENTS
"""
def test_update_multiple_documents():
    try:
        service = Service("elastic_ingest")
        service.start()
        output = service.runtest_update_multiple_documents('localhost', service.flb_listener_port,'update_multiple_documents.json')
        logger.info(f"response: {output}")
        service.stop()
        assert len(output) == 1

        # Verify response details
        for response in output:
            assert response['status'] == 200
            assert response['reason'] == 'OK'
            assert response['data'] == '{"errors":true,"items":[{"delete":{"status":200,"result":"updated"}}]}'
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        service.stop()
        raise

"""
DELETE MULTIPLE DOCUMENTS
"""
def test_delete_multiple_documents():
    try:
        service = Service("elastic_ingest")
        service.start()
        output = service.runtest_delete_multiple_documents('localhost', service.flb_listener_port,'delete_multiple_documents.json')
        logger.info(f"response: {output}")
        service.stop()
        assert len(output) == 1

        # Verify response details
        for response in output:
            assert response['status'] == 200
            assert response['reason'] == 'OK'
            assert response['data'] == '{"errors":true,"items":[{"update":{"status":200,"result":"deleted"}}]}'
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        service.stop()
        raise

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
        #self.flb_listener_port = find_available_port(starting_port=45000)
        self.flb_listener_port = find_available_port(starting_port=45000)

        os.environ['FLUENT_BIT_TEST_LISTENER_PORT'] = str(self.flb_listener_port)
        logger.info(f"Fluent Bit listener port: {self.flb_listener_port}")

        # find and set a listener port for the local server to receive the
        # Fluent Bit output
        self.test_suite_http_port = find_available_port(starting_port=50000)
        os.environ['TEST_SUITE_HTTP_PORT'] = str(self.test_suite_http_port)
        logger.info(f"test suite http port: {self.test_suite_http_port}")

        # Start Fluent Bit
        self.flb.start()

    def runtest_create_index(self,server, port, json_filename):
        conn = create_connection(server, port)
        headers = create_headers()
        json_payload = create_payload(json_filename)
        responses = send_requests(conn, 1, headers, json_payload)
        conn.close()
        return responses

    def runtest_create_multiple_documents(self,server, port, json_filename):
        conn = create_connection(server, port)
        headers = create_headers()
        json_payload = create_payload(json_filename)
        responses = send_requests(conn, 1, headers, json_payload)
        conn.close()
        return responses

    def runtest_update_multiple_documents(self,server, port, json_filename):
        conn = create_connection(server, port)
        headers = create_headers()
        json_payload = create_payload(json_filename)
        responses = send_requests(conn, 1, headers, json_payload)
        conn.close()
        return responses

    def runtest_delete_multiple_documents(self,server, port, json_filename):
        conn = create_connection(server, port)
        headers = create_headers()
        json_payload = create_payload(json_filename)
        responses = send_requests(conn, 1, headers, json_payload)
        conn.close()
        return responses

    def stop(self):
        self.flb.stop()

