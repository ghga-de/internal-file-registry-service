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

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from internal_file_registry_service import models
from internal_file_registry_service.dao import db_models

SOME_DATE = datetime.now()
MIGRATION_DIR = "/workspace/db_migration/alembic"


def hash_string(str_: str):
    """md5 hashes a string"""
    return md5(str_.encode("utf8")).hexdigest()


PREPOPULATED_FILE_FIXTURES = [
    models.FileObjectExternal(
        external_id="GHGAF-02143324934345",
        md5_checksum=hash_string("something to hash 1"),
        size=1000,
    ),
    models.FileObjectExternal(
        external_id="GHGAF-23429923423423",
        md5_checksum=hash_string("something to hash 2"),
        size=2000,
    ),
]


ADDITIONAL_FILE_FIXTURES = [
    models.FileObjectExternal(
        external_id="GHGAF-29992342342234",
        md5_checksum=hash_string("something to hash 3"),
        size=3000,
    ),
    models.FileObjectExternal(
        external_id="GHGAF-50098123865883",
        md5_checksum=hash_string("something to hash 4"),
        size=4000,
    ),
]


def populate_db(db_url: str):
    """Create and populates the DB"""

    # setup database and tables:
    run_migrations(MIGRATION_DIR, db_url)
    engine = create_engine(db_url)

    # populate with test data:
    session_factor = sessionmaker(engine)
    with session_factor() as session:
        for entry in PREPOPULATED_FILE_FIXTURES:
            param_dict = {**entry.dict(), "registration_date": datetime.now()}
            orm_entry = db_models.FileObject(**param_dict)
            session.add(orm_entry)
        session.commit()


from alembic import command
from alembic.config import Config


def run_migrations(script_location: str, dsn: str) -> None:
    alembic_cfg = Config()
    alembic_cfg.set_main_option("script_location", script_location)
    alembic_cfg.set_main_option("sqlalchemy.url", dsn)
    command.upgrade(alembic_cfg, "head")
