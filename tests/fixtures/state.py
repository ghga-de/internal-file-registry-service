# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from ghga_service_chassis_lib.object_storage_dao_testing import ObjectFixture, calc_md5
from ghga_service_chassis_lib.utils import TEST_FILE_PATHS

from internal_file_registry_service import models

from .config import DEFAULT_CONFIG


def get_study_id_example(index: int) -> str:
    "Generate an example study ID."
    return f"mystudy-{index}"


def get_file_id_example(index: int) -> str:
    "Generate an example file ID."
    return f"myfile-{index}"


class FileState:
    def __init__(
        self,
        id: str,
        grouping_label: str,
        creation_date: datetime,
        update_date: datetime,
        size: int,
        format: str,
        file_path: Path,
        in_permanent_storage: bool,
        in_inbox: bool,
        in_outbox: bool,
        populate_db: bool = True,
        populate_storage: bool = True,
        message: Optional[dict] = None,
    ):
        """
        Initialize file state and create imputed attributes.
        You may set `populate_db` or `populate_storage` to `False` to indicate that this
        file should not be added to the database or the storage respectively.
        """
        self.id = id
        self.grouping_label = grouping_label
        self.creation_date = creation_date
        self.update_date = update_date
        self.size = size
        self.format = format
        self.file_path = file_path
        self.in_permanent_storage = in_permanent_storage
        self.in_inbox = in_inbox
        self.in_outbox = in_outbox
        self.message = message
        self.populate_db = populate_db
        self.populate_storage = populate_storage

        # computed attributes:
        with open(self.file_path, "rb") as file:
            self.content = file.read()

        self.md5 = calc_md5(self.content)
        self.file_info = models.FileInfoInitial(
            file_id=self.id,
            grouping_label=self.grouping_label,
            creation_date=self.creation_date,
            update_date=self.update_date,
            size=self.size,
            format=self.format,
            md5_checksum=self.md5,
        )

        self.storage_objects: List[ObjectFixture] = []
        if self.in_permanent_storage:
            self.storage_objects.append(
                ObjectFixture(
                    file_path=self.file_path,
                    bucket_id=self.grouping_label,
                    object_id=self.id,
                )
            )
        if self.in_inbox:
            self.storage_objects.append(
                ObjectFixture(
                    file_path=self.file_path,
                    bucket_id=DEFAULT_CONFIG.s3_inbox_bucket_id,
                    object_id=self.id,
                )
            )
        if self.in_outbox:
            self.storage_objects.append(
                ObjectFixture(
                    file_path=self.file_path,
                    bucket_id=DEFAULT_CONFIG.s3_outbox_bucket_id,
                    object_id=self.id,
                )
            )


test_time = datetime.now(timezone.utc)
test_time_string = test_time.isoformat()

FILES: Dict[str, FileState] = {
    "in_registry": FileState(
        id=get_file_id_example(0),
        grouping_label=get_study_id_example(0),
        creation_date=test_time,
        update_date=test_time,
        size=0,
        format=".test",
        file_path=TEST_FILE_PATHS[0],
        in_permanent_storage=True,
        in_inbox=False,
        in_outbox=False,
        message={
            "file_id": get_file_id_example(0),
            "grouping_label": get_study_id_example(0),
            "md5_checksum": "3851c5cb7518a2ff67ab5581c3e01f2f",  # fake checksum
            "creation_date": test_time_string,
            "update_date": test_time_string,
            "size": 0,
            "format": ".test",
        },
    ),
    "in_registry_and_outbox": FileState(
        id=get_file_id_example(1),
        grouping_label=get_study_id_example(0),
        creation_date=test_time,
        update_date=test_time,
        size=0,
        format=".test",
        file_path=TEST_FILE_PATHS[1],
        in_permanent_storage=True,
        in_inbox=False,
        in_outbox=True,
    ),
    "in_inbox_only": FileState(
        id=get_file_id_example(2),
        grouping_label=get_study_id_example(1),
        creation_date=test_time,
        update_date=test_time,
        size=0,
        format=".test",
        file_path=TEST_FILE_PATHS[2],
        in_permanent_storage=False,
        in_inbox=True,
        in_outbox=False,
        message={
            "file_id": get_file_id_example(2),
            "grouping_label": get_study_id_example(1),
            "md5_checksum": "3851c5cb7518a2ff67ab5581c3e01f2f",  # fake checksum
            "creation_date": test_time_string,
            "update_date": test_time_string,
            "size": 0,
            "format": ".test",
        },
    ),
    "db_missing": FileState(
        id=get_file_id_example(100),
        grouping_label=get_study_id_example(0),
        creation_date=test_time,
        update_date=test_time,
        size=0,
        format=".test",
        file_path=TEST_FILE_PATHS[0],
        in_permanent_storage=True,
        in_inbox=False,
        in_outbox=False,
        populate_db=False,
    ),
    "in_inbox_and_reg_but_db_missing": FileState(
        id=get_file_id_example(101),
        grouping_label=get_study_id_example(0),
        creation_date=test_time,
        update_date=test_time,
        size=0,
        format=".test",
        file_path=TEST_FILE_PATHS[0],
        in_permanent_storage=True,
        in_inbox=True,
        in_outbox=False,
        populate_db=False,
    ),
    "storage_missing": FileState(
        id=get_file_id_example(102),
        grouping_label=get_study_id_example(0),
        creation_date=test_time,
        update_date=test_time,
        size=0,
        format=".test",
        file_path=TEST_FILE_PATHS[0],
        in_permanent_storage=True,
        in_inbox=False,
        in_outbox=False,
        populate_storage=False,
    ),
    "exists_nowhere": FileState(
        id=get_file_id_example(200),
        grouping_label=get_study_id_example(100),
        creation_date=test_time,
        update_date=test_time,
        size=0,
        format=".test",
        file_path=TEST_FILE_PATHS[0],
        in_permanent_storage=False,
        in_inbox=False,
        in_outbox=False,
    ),
    "no_grouping_label_in_message": FileState(
        id=get_file_id_example(300),
        grouping_label=get_study_id_example(100),
        creation_date=test_time,
        update_date=test_time,
        size=0,
        format=".test",
        file_path=TEST_FILE_PATHS[0],
        in_permanent_storage=True,
        in_inbox=False,
        in_outbox=False,
        message={
            "file_id": get_file_id_example(0),
            "md5_checksum": "3851c5cb7518a2ff67ab5581c3e01f2f",  # fake checksum
            "creation_date": test_time_string,
            "update_date": test_time_string,
            "size": 0,
            "format": ".test",
        },
    ),
}
