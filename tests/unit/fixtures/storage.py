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

"""Fixtures for testing the storage DAO"""

from ghga_service_chassis_lib.object_storage_dao_testing import ObjectFixture

from .utils import BASE_DIR

EXISTING_OBJECT = ObjectFixture(
    file_path=BASE_DIR / "test_file1.yaml",
    bucket_id="ghgas-9992324243",
    object_id="ghgaf-8234659943",
)

NOT_EXISTING_OBJECT = ObjectFixture(
    file_path=BASE_DIR / "test_file2.yaml",
    bucket_id="ghgas-3440345533",
    object_id="ghgaf-5226343543",
)
