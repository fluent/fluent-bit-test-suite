import http.client, time, json, os, ssl, logging, requests

from utils.fluent_bit_manager import FluentBitManager
from utils.network import find_available_port
from server.otlp_server import  data_storage

logger = logging.getLogger(__name__)

def create_connection_tls_on(server, port, cafile):
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=cafile)
    logger.info('Certificate loaded...')
    return http.client.HTTPSConnection(server, port, context=context)

def create_connection(server, port):
    return http.client.HTTPConnection(server, port)


def create_headers():
    return {
        'Connection': 'keepalive',
        'Authorization': 'Splunk secret-token',
        'Content-Type': 'application/json'
    }

def create_payload():
    return json.dumps({

        "Event": "Some text in the event"
    })

def send_requests(conn, num_requests, headers, json_payload):
    responses = []
    for i in range(num_requests):
        conn.request("POST", "/services/collector", body=json_payload, headers=headers)
        response = conn.getresponse()
        responses.append({
            'status': response.status,
            'reason': response.reason,
            'data': response.read().decode()
        })
    return responses

def test_splunk_on_http2_keepalive_tls_on():
    try:
        service = Service("splunk_on_http2_keepalive_tls_on.yaml")
        service.start()
        certificate_path = f'{service.test_path}/../certificate/certificate.pem'
        logger.info(f'certificate path: {certificate_path}')
        output = service.run_splunk_tls('localhost', service.flb_listener_port, certificate_path, 3)
        logger.info(f"response: {output}")
        service.stop()
        assert len(output) == 3

        # Verify response details
        for response in output:
            assert response['status'] == 200
            assert response['reason'] == 'OK'
            assert response['data'] == '{"text":"Success","code":0}'
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if service.flb.process is not None:
            service.stop()
        raise

def test_splunk_on_http2_no_keepalive():
    try:
        service = Service("splunk_on_http2_no_keepalive.yaml")
        logger.info(f"service: {service}")
        service.start()
        certificate_path = f'{service.test_path}/../certificate/certificate.pem'
        logger.info(f'certificate path: {certificate_path}')
        output = service.run_splunk_tls('localhost', service.flb_listener_port, certificate_path, 1)
        logger.info(f"response: {output}")
        service.stop()
        assert len(output) == 1

        # Verify response details
        for response in output:
            assert response['status'] == 200
            assert response['reason'] == 'OK'
            assert response['data'] == '{"text":"Success","code":0}'
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if service.flb.process is not None:
            service.stop()
        raise

def test_splunk_on_http2_keepalive():
    try:
        logger.info('>>>>>> abriendo')
        logger.info('>>>>>> abriendo')
        logger.info('>>>>>> abriendo')
        logger.info('>>>>>> abriendo')
        service = Service("splunk_on_http2_keepalive.yaml")
        service.start()
        certificate_path = f'{service.test_path}/../certificate/certificate.pem'
        output = service.run_splunk_tls('localhost', service.flb_listener_port, certificate_path, 3)
        logger.info(f"response: {output}")
        service.stop()
        assert len(output) == 3

        # Verify response details
        for response in output:
            assert response['status'] == 200
            assert response['reason'] == 'OK'
            assert response['data'] == '{"text":"Success","code":0}'
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if service.flb.process is not None:
            service.stop()
        raise




def test_splunk_on_http2_on_keepalive_on_tls_on():
    try:
        service = Service("splunk_on_http2_on_keepalive_on_tls_on.yaml")
        logger.info(f"service: {service}")
        service.start()
        certificate_path=f'{service.test_path}/../certificate/certificate.pem'
        logger.info(f'certificate path: {certificate_path}')
        output = service.run_splunk_tls('localhost', service.flb_listener_port, certificate_path, 3)
        logger.info(f"response: {output}")
        service.stop()
        assert len(output) == 3

        # Verify response details
        for response in output:
            assert response['status'] == 200
            assert response['reason'] == 'OK'
            assert response['data'] == '{"text":"Success","code":0}'
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if service.flb.process is not None:
            service.stop()
        raise

class Service:
    def __init__(self, config_file):
        # Compose the absolute path for the Fluent Bit configuration file
        self.test_path = os.path.dirname(os.path.abspath(__file__))
        self.config_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '../config/', config_file))

        # Compose the Certificate path
        self.tls_crt_file= f'{self.test_path}/../certificate/certificate.pem'
        self.tls_key_file= f'{self.test_path}/../certificate/private_key.pem'

        logger.info(f'Config filename :{self.config_file}')
        data_storage['logs'] = []

    def start(self):
        # Create an instance of the Fluent Bit Manager
        self.flb = FluentBitManager(self.config_file)

        # set a listener port (input plugin) for the config file (note: we use an environment variable
        # so Fluent Bit will replace it in the config once it loads it)
        self.flb_listener_port = find_available_port(starting_port=45000)
        os.environ['FLUENT_BIT_TEST_LISTENER_PORT'] = str(self.flb_listener_port)
        logger.info(f"Fluent Bit listener port: {self.flb_listener_port}")
        os.environ['CERTIFICATE_TEST'] = str(self.tls_crt_file)
        os.environ['PRIVATE_KEY_TEST'] = str(self.tls_key_file)
        # set certificate and rivate_key

        # find and set a listener port for the local server to receive the
        # Fluent Bit output
        self.test_suite_http_port = find_available_port(starting_port=50000)
        os.environ['TEST_SUITE_HTTP_PORT'] = str(self.test_suite_http_port)
        logger.info(f"test suite http port: {self.test_suite_http_port}")

        # Start Fluent Bit
        self.flb.start()

    def run_splunk_tls(self,server, port, cafile, num_requests):
        conn = create_connection_tls_on(server, port, cafile)
        logger.info(f'connection = {conn}')
        headers = create_headers()
        logger.info(f'headers = {headers}')
        json_payload = create_payload()
        logger.info(f'json_payload = {json_payload}')
        responses = send_requests(conn, num_requests, headers, json_payload)
        conn.close()
        return responses

    def run_splunk(self,server, port, num_requests):
        conn = create_connection(server, port)
        headers = create_headers()
        json_payload = create_payload()
        responses = send_requests(conn, num_requests, headers, json_payload)
        conn.close()
        return responses



    def stop(self):
        self.flb.stop()
        #requests.post(f'http://localhost:{self.test_suite_http_port}/shutdown')

