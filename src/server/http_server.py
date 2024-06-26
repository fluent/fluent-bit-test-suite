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

app = Flask(__name__)
data_storage = {"payloads": []}
logger = logging.getLogger(__name__)

@app.route('/data', methods=['POST'])
def receive_data():
    data = request.json
    data_storage["payloads"].append(data)
    return jsonify({"status": "received"}), 200

def http_server_run(port=60000):
    logger.info("Starting HTTP server")
    app.run(port=port)
