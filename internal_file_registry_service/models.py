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

"""Defines dataclasses for holding business-logic data"""

from datetime import datetime

from ghga_service_chassis_lib.object_storage_dao import (
    BucketIdValidationError,
    ObjectIdValidationError,
    validate_bucket_id,
    validate_object_id,
)
from pydantic import UUID4, BaseModel, validator


class FileInfoExternal(BaseModel):
    """
    A model for communicating file info to external services.
    This is missing the internal file ID `id` and the grouping label
    """

    file_id: str
    md5_checksum: str
    creation_date: datetime
    update_date: datetime
    size: int
    format: str

    # pylint: disable=no-self-argument,no-self-use
    @validator("file_id")
    def check_file_id(cls, value: str):
        """Checks if the file_id is valid for use as a s3 object id."""

        try:
            validate_object_id(value)
        except ObjectIdValidationError as error:
            raise ValueError(
                f"External ID '{value}' cannot be used as a (S3) object id."
            ) from error

        return value

    class Config:
        """Additional pydantic configs."""

        orm_mode = True


class FileInfoInitial(FileInfoExternal):
    """
    A model for processing files, includes all file info needed to add a file
    to the database and storage
    """

    grouping_label: str

    # pylint: disable=no-self-argument,no-self-use
    @validator("grouping_label")
    def check_grouping_label(cls, value: str):
        """Checks if the grouping_label is valid for use as a (S3) bucket label."""

        value_casted = value.lower()

        try:
            validate_bucket_id(value_casted)
        except BucketIdValidationError as error:
            raise ValueError(
                f"Grouping label '{value}' cannot be casted into a bucket id."
            ) from error

        return value_casted


class FileInfoComplete(FileInfoInitial):
    """
    A model for describing all file info.
    Only intended for service-internal use.
    """

    id: UUID4
