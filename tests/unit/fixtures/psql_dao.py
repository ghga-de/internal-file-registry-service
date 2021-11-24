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

"""Fixtures for testing the PostgreSQL functionalities"""

from datetime import datetime
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from internal_file_registry_service import models
from internal_file_registry_service.dao import db_models

from .storage import EXISTING_OBJECT, NOT_EXISTING_OBJECT

EXISTING_FILE_INFO = models.FileInfoExternal(
    external_id=EXISTING_OBJECT.object_id,
    grouping_label=EXISTING_OBJECT.bucket_id,
    md5_checksum=EXISTING_OBJECT.md5,
    size=1000,  # not the real size
)

NOT_EXISTING_FILE_INFO = models.FileInfoExternal(
    external_id=NOT_EXISTING_OBJECT.object_id,
    grouping_label=NOT_EXISTING_OBJECT.bucket_id,
    md5_checksum=NOT_EXISTING_OBJECT.md5,
    size=2000,  # not the real size
)


def populate_db(db_url: str, fixtures: List[models.FileInfoExternal]):
    """Create and populates the DB"""

    # setup database and tables:
    engine = create_engine(db_url)
    db_models.Base.metadata.create_all(engine)

    # populate with test data:
    session_factor = sessionmaker(engine)
    with session_factor() as session:
        for fixture in fixtures:
            param_dict = {**fixture.dict(), "registration_date": datetime.now()}
            orm_entry = db_models.FileInfo(**param_dict)
            session.add(orm_entry)
        session.commit()
