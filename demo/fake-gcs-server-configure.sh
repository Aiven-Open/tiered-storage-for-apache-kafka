#!/usr/bin/env bash
##
# Copyright 2023 Aiven Oy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##

curl -X PUT http://fake-gcs-server:4443/_internal/config \
     -H "Content-Type: application/json" \
     -d '{"externalUrl": "http://fake-gcs-server:4443"}'

curl -X POST http://fake-gcs-server:4443/storage/v1/b?project=test-project \
     -H "Content-Type: application/json" \
     -d '{"name": "test-bucket"}'
