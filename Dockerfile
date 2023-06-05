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
FROM aivenoy/kafka:3.3-2022-10-06-tiered-storage-1

ARG _VERSION

USER root
RUN mkdir /tiered-storage-for-apache-kafka
COPY build/distributions/tiered-storage-for-apache-kafka-${_VERSION}.tgz /tiered-storage-for-apache-kafka
RUN cd /tiered-storage-for-apache-kafka \
    && tar -xf tiered-storage-for-apache-kafka-${_VERSION}.tgz --strip-components=1 \
    && rm tiered-storage-for-apache-kafka-${_VERSION}.tgz

# Restore the user.
USER appuser
