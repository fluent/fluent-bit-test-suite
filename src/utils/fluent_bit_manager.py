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
import time
import shutil
import requests
import subprocess
import logging
import datetime
from utils.network import find_available_port

ENV_FLB_HTTP_MONITORING_PORT = "FLUENT_BIT_HTTP_MONITORING_PORT"

logger = logging.getLogger(__name__)

class FluentBitManager:
    def __init__(self, config_path=None, binary_path='fluent-bit'):
        logger.info(f"config path {config_path}")
        self.config_path = config_path
        self.binary_path = binary_path
        self.binary_absolute_path = shutil.which(binary_path)
        self.process = None

    def set_http_monitoring_port(self, env_var_name, starting_port=40000):
        port = find_available_port(starting_port)
        os.environ[env_var_name] = str(port)
        self.http_monitoring_port = str(port)

    def start(self):
        if not os.path.exists(self.config_path):
            raise FileNotFoundError("Config file {self.config_path} does not exist")

        # create temporary directory for logs
        out_dir = self.create_results_directory()

        log_file = os.path.join(out_dir, f"fluent_bit.log")
        self.set_http_monitoring_port(ENV_FLB_HTTP_MONITORING_PORT)

        version, commit = self.get_version_info()
        logger.info(f'Fluent Bit info')
        logger.info(f' version    : {version}')
        logger.info(f' path       : {self.binary_absolute_path}')
        logger.info(f" config file: {self.config_path}")
        logger.info(f" logfile    : {log_file}")
        logger.info(f" http port  : {self.http_monitoring_port} (testing...)")

        # spawn the process
        self.process = subprocess.Popen([self.binary_path, "-c", self.config_path, "-l", log_file])

        # wait for Fluent Bit to start
        self.wait_for_fluent_bit()

    def stop(self):
        if self.process:
            self.process.terminate()
            self.process.wait()
            self.process = None

    def get_version_info(self):
        try:
            result = subprocess.run([self.binary_path, '--version'], capture_output=True, text=True, check=True)
            output = result.stdout.strip().split('\n')
            version = output[0].replace('Fluent Bit ', '').strip()
            commit = output[1].strip().replace('Git commit: ', '')
            return version, commit
        except subprocess.CalledProcessError as e:
            logger.error("Error running Fluent Bit: %s", e)
            return "Error running Fluent Bit: {e}"

    def create_results_directory(self, base_dir='results'):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        results_dir = os.path.join(base_dir, f"fluent_bit_results_{timestamp}")
        os.makedirs(results_dir, exist_ok=True)
        return results_dir

    # Check if Fluent Bit is running by trying to reach the uptime endpoint, it waits until
    # the value of `uptime_sec` is greater than 1
    def wait_for_fluent_bit(self, timeout=10):
        url = f"http://127.0.0.1:{self.http_monitoring_port}/api/v1/uptime"

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    uptime = response.json().get('uptime_sec', 0)
                    if uptime > 1:
                        logger.info("Fluent Bit is running, health check OK")
                        return True
            except requests.ConnectionError:
                # it's ok to fail, we are testing
                pass

            time.sleep(1)

        logger.error("Fluent Bit did not start within the timeout period")
        return False