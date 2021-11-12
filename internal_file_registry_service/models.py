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

"""Defines dataclasses for holding business-logic data"""

from datetime import datetime

from pydantic import UUID4, BaseModel


class FileObjectExternal(BaseModel):
    """
    A model for communicating file object-related information to external services.
    This is missing the internal file ID `id` as well as the registration date as
    this information shouldn't be shared with other services.
    """

    external_id: str
    md5_encrypted: str
    md5_decrypted: str

    class Config:
        """Additional pydantic configs."""

        orm_mode = True


class FileObjectComplete(FileObjectExternal):
    """
    A model for describing all file object-related information.
    Only intended for service-internal use.
    """

    id: UUID4
    registration_date: datetime
