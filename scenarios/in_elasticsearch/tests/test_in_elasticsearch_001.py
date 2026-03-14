import http.client, json, os, logging

import pytest

from server.otlp_server import data_storage
from utils.http_matrix import PROTOCOL_CASES, run_curl_request
from utils.test_service import FluentBitTestService

logger = logging.getLogger(__name__)

IN_ELASTICSEARCH_PROTOCOL_CONFIGS = {
    "http1_cleartext": "in_elasticsearch_http1_cleartext.yaml",
    "http2_cleartext": "in_elasticsearch_http2_cleartext.yaml",
    "http1_tls": "in_elasticsearch_http1_tls.yaml",
    "http2_tls": "in_elasticsearch_http2_tls.yaml",
}


def parse_single_item_response(response_text):
    payload = json.loads(response_text)
    assert "items" in payload
    assert len(payload["items"]) == 1
    operation, details = next(iter(payload["items"][0].items()))
    return payload, operation, details

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
def send_request(conn, method, path, headers=None, body=None):
    conn.request(method, path, body=body, headers=headers or {})
    response = conn.getresponse()
    return {
        'status': response.status,
        'reason': response.reason,
        'data': response.read().decode()
    }


def test_in_elasticsearch_root_info():
    service = Service("in_elasticsearch")
    service.start()
    conn = create_connection('localhost', service.flb_listener_port)
    response = send_request(conn, "GET", "/")
    conn.close()
    service.stop()

    assert response['status'] == 200
    assert response['reason'] == 'OK'
    assert '"version"' in response['data']


def test_in_elasticsearch_nodes_http():
    service = Service("in_elasticsearch")
    service.start()
    conn = create_connection('localhost', service.flb_listener_port)
    response = send_request(conn, "GET", "/_nodes/http")
    conn.close()
    service.stop()

    assert response['status'] == 200
    assert response['reason'] == 'OK'
    assert '"_nodes"' in response['data']
    assert '"nodes"' in response['data']


def test_in_elasticsearch_create_index():
    try:
        service = Service("in_elasticsearch")
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
        if service.flb.process is not None:
            service.stop()
        raise

"""
CREATE MULTIPLOE DOCUMENTS
"""
def test_in_elasticsearch_create_multiple_documents():
    try:
        service = Service("in_elasticsearch")
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
        if service.flb.process is not None:
            service.stop()
        raise

"""
UOPDATE MULTIPLE DOCUMENTS
"""
def test_in_elasticsearch_update_multiple_documents():
    try:
        service = Service("in_elasticsearch")
        service.start()
        output = service.runtest_update_multiple_documents('localhost', service.flb_listener_port,'update_multiple_documents.json')
        logger.info(f"response: {output}")
        service.stop()
        assert len(output) == 1

        # Verify response details
        for response in output:
            assert response['status'] == 200
            assert response['reason'] == 'OK'
            payload, operation, details = parse_single_item_response(response['data'])
            assert payload["errors"] is True
            assert operation == "delete"
            assert details["status"] == 404
            assert details["result"] == "not_found"
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if service.flb.process is not None:
            service.stop()
        raise

"""
DELETE MULTIPLE DOCUMENTS
"""
def test_in_elasticsearch_delete_multiple_documents():
    try:
        service = Service("in_elasticsearch")
        service.start()
        output = service.runtest_delete_multiple_documents('localhost', service.flb_listener_port,'delete_multiple_documents.json')
        service.stop()
        logger.info(f"response: {output}")
        assert len(output) == 1

        # Verify response details
        for response in output:
            assert response['status'] == 200
            assert response['reason'] == 'OK'
            payload, operation, details = parse_single_item_response(response['data'])
            assert payload["errors"] is True
            assert operation == "update"
            assert details["status"] == 403
            assert details["result"] == "forbidden"
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if service.flb.process is not None:
            service.stop()
        raise


@pytest.mark.parametrize("case", PROTOCOL_CASES, ids=[case["id"] for case in PROTOCOL_CASES])
def test_in_elasticsearch_bulk_protocol_matrix(case):
    service = Service(IN_ELASTICSEARCH_PROTOCOL_CONFIGS[case["config_key"]])
    service.start()

    scheme = "https" if case["use_tls"] else "http"
    result = run_curl_request(
        f"{scheme}://localhost:{service.flb_listener_port}/_bulk",
        create_payload("create_index.json"),
        headers=["Content-Type: application/json"],
        http_mode=case["http_mode"],
        ca_cert_path=service.tls_crt_file if case["use_tls"] else None,
    )

    service.stop()

    assert result["status_code"] == 200
    assert result["http_version"] == case["expected_http_version"]
    payload, operation, details = parse_single_item_response(result["body"])
    assert payload["errors"] is False
    assert operation == "index"
    assert details["status"] == 201
    assert details["result"] == "created"


def test_in_elasticsearch_rejects_unknown_bulk_operation():
    service = Service("in_elasticsearch")
    service.start()

    result = run_curl_request(
        f"http://localhost:{service.flb_listener_port}/_bulk",
        '{"nonexistent":{"_index":"fluent-bit","_id":"1"}}\n{"message":"hello"}\n',
        headers=["Content-Type: application/json"],
        http_mode="http1.1",
    )

    service.stop()

    assert result["status_code"] == 200
    payload, operation, details = parse_single_item_response(result["body"])
    assert payload["errors"] is True
    assert operation == "unknown"
    assert details["status"] == 400


class Service:
    def __init__(self, config_file):
        # Compose the absolute path for the Fluent Bit configuration file
        self.config_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '../config/', config_file))
        test_path = os.path.dirname(os.path.abspath(__file__))
        cert_dir = os.path.abspath(os.path.join(test_path, "../../in_splunk/certificate"))
        self.tls_crt_file = os.path.join(cert_dir, "certificate.pem")
        self.tls_key_file = os.path.join(cert_dir, "private_key.pem")
        self.service = FluentBitTestService(
            self.config_file,
            data_storage=data_storage,
            data_keys=["logs"],
            extra_env={
                "CERTIFICATE_TEST": self.tls_crt_file,
                "PRIVATE_KEY_TEST": self.tls_key_file,
            },
        )

    def start(self):
        self.service.start()
        self.flb = self.service.flb
        self.flb_listener_port = self.service.flb_listener_port
        self.test_suite_http_port = self.service.test_suite_http_port
        logger.info(f"Fluent Bit listener port: {self.flb_listener_port}")
        logger.info(f"test suite http port: {self.test_suite_http_port}")

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
        self.service.stop()
