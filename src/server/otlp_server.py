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

import logging
from flask import Flask, request, jsonify
from google.protobuf import json_format
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest

app = Flask(__name__)
data_storage = {"traces": [], "metrics": [], "logs": []}
logger = logging.getLogger(__name__)

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

    #logger.info(f"Received log:\n {json_format.MessageToJson(log_request)}")
    return jsonify({"status": "received"}), 200

def otlp_server_run(port=4317):
    logger.info("Starting OTLP server")
    app.run(port=port)
