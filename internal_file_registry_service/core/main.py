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

"""Main business-logic of this service"""

from ..config import CONFIG, Config
from ..dao import Database, ObjectAlreadyExistsError, ObjectStorage


class FileAlreadyOnStageError(RuntimeError):
    """Thrown when there was a stage request for an already staged file."""

    def __init__(self, external_file_id: str):
        message = f"The file with external id {external_file_id} is already staged."
        super().__init__(message)


def stage_file(external_file_id: str, config: Config = CONFIG) -> None:
    """Copies a file into the stage bucket."""

    # get file info from the database:
    # (will throw an error if file not in registry)
    with Database(config=config) as database:
        file_info = database.get_file_info(external_file_id)

    # copy file object to the stage bucket:
    with ObjectStorage(config=config) as storage:
        try:
            storage.copy_object(
                source_bucket_id=file_info.grouping_label,
                source_object_id=external_file_id,
                dest_bucket_id=config.s3_stage_bucket_id,
                dest_object_id=external_file_id,
            )
        except ObjectAlreadyExistsError as error:
            raise FileAlreadyOnStageError(external_file_id=external_file_id) from error
