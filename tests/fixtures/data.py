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

"""Test data"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from ghga_service_chassis_lib.object_storage_dao_testing import ObjectFixture, calc_md5
from ghga_service_chassis_lib.utils import TEST_FILE_PATHS, create_fake_drs_uri

from internal_file_registry_service import models

from .config import DEFAULT_CONFIG

EXAMPLE_STUDY_IDS = ["mystudy-11111111111", "mystudy-22222222222"]
EXAMPLE_FILE_IDS = ["myfile-11111111", "myfile-222222222", "myfile-3333333333"]


class FileFixture:
    def __init__(
        self,
        id: str,
        grouping_label: str,
        file_path: Path,
        in_permanent_storage: bool,
        in_inbox: bool,
        in_outbox: bool,
        message: Optional[dict] = None,
    ):
        """initialize file fixture and create imputed attributes"""
        self.id = id
        self.grouping_label = grouping_label
        self.file_path = file_path
        self.in_permanent_storage = in_permanent_storage
        self.in_inbox = in_inbox
        self.in_outbox = in_outbox
        self.message = message

        # computed attributes:
        with open(self.file_path, "rb") as file:
            self.content = file.read()

        self.md5 = calc_md5(self.content)
        self.file_info = models.FileInfoExternal(
            external_id=self.id,
            grouping_label=self.grouping_label,
            md5_checksum=self.md5,
            size=1000,  # not the real size
        )

        self.object_fixtures: List[ObjectFixture] = []
        if self.in_permanent_storage:
            self.object_fixtures.append(
                ObjectFixture(
                    file_path=self.file_path,
                    bucket_id=self.grouping_label,
                    object_id=self.id,
                )
            )
        if self.in_inbox:
            self.object_fixtures.append(
                ObjectFixture(
                    file_path=self.file_path,
                    bucket_id=DEFAULT_CONFIG.s3_inbox_bucket_id,
                    object_id=self.id,
                )
            )
        if self.in_outbox:
            self.object_fixtures.append(
                ObjectFixture(
                    file_path=self.file_path,
                    bucket_id=DEFAULT_CONFIG.s3_outbox_bucket_id,
                    object_id=self.id,
                )
            )


FILE_FIXTURES: Dict[str, FileFixture] = {
    "in_registry": FileFixture(
        id=EXAMPLE_FILE_IDS[0],
        grouping_label=EXAMPLE_STUDY_IDS[0],
        file_path=TEST_FILE_PATHS[0],
        in_permanent_storage=True,
        in_inbox=False,
        in_outbox=False,
        message={
            "drs_id": create_fake_drs_uri(EXAMPLE_FILE_IDS[0]),
            "file_id": EXAMPLE_FILE_IDS[0],
            "grouping_label": EXAMPLE_STUDY_IDS[0],
            "request_id": "my_test_stage_request_001",
            "timestamp": datetime.now().isoformat(),
        },
    ),
    "in_registry_and_outbox": FileFixture(
        id=EXAMPLE_FILE_IDS[1],
        grouping_label=EXAMPLE_STUDY_IDS[0],
        file_path=TEST_FILE_PATHS[1],
        in_permanent_storage=True,
        in_inbox=False,
        in_outbox=True,
    ),
    "in_inbox_only": FileFixture(
        id=EXAMPLE_FILE_IDS[2],
        grouping_label=EXAMPLE_STUDY_IDS[1],
        file_path=TEST_FILE_PATHS[3],
        in_permanent_storage=False,
        in_inbox=True,
        in_outbox=False,
    ),
}
