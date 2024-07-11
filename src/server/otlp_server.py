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

import time
import logging
import threading

from flask import Flask, request, jsonify
from waitress import serve
from google.protobuf import json_format
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest

app = Flask(__name__)
data_storage = {"traces": [], "metrics": [], "logs": []}
logger = logging.getLogger(__name__)

server_thread = None
shutdown_flag = threading.Event()

from flask import Flask, request, jsonify
import threading
import time
import requests
import logging
from waitress import serve

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

shutdown_flag = threading.Event()

@app.route('/shutdown', methods=['POST'])
def shutdown():
    shutdown_flag.set()
    logger.info("OTLP Server is shutting down...")
    return jsonify({"status": "shutting down"}), 200

@app.route('/v1/traces', methods=['POST'])
def traces():
    data = request.data
    trace_request = ExportTraceServiceRequest()
    trace_request.ParseFromString(data)
    data_storage["traces"].append(trace_request)
    return jsonify({"status": "received"}), 200

@app.route('/v1/metrics', methods=['POST'])
def metrics():
    data = request.data
    metric_request = ExportMetricsServiceRequest()
    metric_request.ParseFromString(data)
    data_storage["metrics"].append(metric_request)
    return jsonify({"status": "received"}), 200

@app.route('/v1/logs', methods=['POST'])
def logs():
    data = request.data
    log_request = ExportLogsServiceRequest()
    log_request.ParseFromString(data)
    data_storage["logs"].append(log_request)
    return jsonify({"status": "received"}), 200

@app.route('/ping', methods=['GET'])
def ping():
    return jsonify({"status": "pong"}), 200

def run_server(port=4317):
    serve(app, host='0.0.0.0', port=port, threads=1)

def otlp_server_run(port):
    global server_thread

    logger.info(f"Starting OTLP server on port {port}")
    server_thread = threading.Thread(target=run_server, kwargs={"port": port})
    server_thread.daemon = True
    server_thread.start()

    return server_thread
