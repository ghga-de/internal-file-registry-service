# Copyright 2021 Universität Tübingen, DKFZ and EMBL
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Pub sub fixtures"""

from datetime import datetime

from ghga_service_chassis_lib.pubsub_testing import amqp_fixture_factory
from ghga_service_chassis_lib.utils import create_fake_drs_uri

from .config import DEFAULT_CONFIG
from .storage import EXISTING_OBJECTS

TEST_MESSAGES = {
    "non_staged_file_requested": [
        {
            "drs_id": create_fake_drs_uri(EXISTING_OBJECTS[0].object_id),
            "file_id": EXISTING_OBJECTS[0].object_id,
            "grouping_label": EXISTING_OBJECTS[0].object_id,
            "request_id": "my_test_stage_request_001",
            "timestamp": datetime.now().isoformat(),
        }
    ]
}


amqp_fixture = amqp_fixture_factory(service_name=DEFAULT_CONFIG.service_name)
