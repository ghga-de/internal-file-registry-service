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
from hashlib import md5

from ghga_service_chassis_lib.postgresql import PostgresqlConfigBase
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from internal_file_registry_service import models
from internal_file_registry_service.dao import db_models

SOME_DATE = datetime.now()


def hash_string(str_: str):
    """md5 hashes a string"""
    return md5(str_.encode("utf8")).hexdigest()


PREPOPULATED_FILE_FIXTURES = [
    models.FileObjectExternal(
        external_id="GHGAF-02143324934345",
        md5_encrypted=hash_string("something to hash 1"),
        md5_decrypted=hash_string("something to hash 2"),
    ),
    models.FileObjectExternal(
        external_id="GHGAF-23429923423423",
        md5_encrypted=hash_string("something to hash 3"),
        md5_decrypted=hash_string("something to hash 4"),
    ),
]


ADDITIONAL_FILE_FIXTURES = [
    models.FileObjectExternal(
        external_id="GHGAF-29992342342234",
        md5_encrypted=hash_string("something to hash 5"),
        md5_decrypted=hash_string("something to hash 6"),
    ),
    models.FileObjectExternal(
        external_id="GHGAF-50098123865883",
        md5_encrypted=hash_string("something to hash 7"),
        md5_decrypted=hash_string("something to hash 8"),
    ),
]


def populate_db(db_url: str):
    """Create and populates the DB"""

    # setup database and tables:
    engine = create_engine(db_url)
    db_models.Base.metadata.create_all(engine)

    # populate with test data:
    session_factor = sessionmaker(engine)
    with session_factor() as session:
        for entry in PREPOPULATED_FILE_FIXTURES:
            param_dict = {**entry.dict(), "registration_date": datetime.now()}
            orm_entry = db_models.FileObject(**param_dict)
            session.add(orm_entry)
        session.commit()


def config_from_psql_container(container: PostgresContainer) -> PostgresqlConfigBase:
    """Prepares a PostgresqlConfigBase from an instance of
    postgres test container."""
    db_url = container.get_connection_url()
    db_url_formatted = db_url.replace("postgresql+psycopg2", "postgresql")
    return PostgresqlConfigBase(db_url=db_url_formatted)
