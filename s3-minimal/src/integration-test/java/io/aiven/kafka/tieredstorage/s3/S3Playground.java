/*
 * Copyright 2021 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage.s3;

import java.util.Map;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectResult;

import static io.aiven.kafka.tieredstorage.s3.S3TieredStorageConfig.AWS_REGION_CONFIG;
import static io.aiven.kafka.tieredstorage.s3.S3TieredStorageConfig.S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.tieredstorage.s3.S3TieredStorageConfig.S3_OBJECT_PREFIX_CONFIG;

public class S3Playground {
    public static void main(final String[] args) {
        final Map<String, ?> props = Map.of(
            S3_BUCKET_NAME_CONFIG, "aiven-jeqo-test1",
            S3_OBJECT_PREFIX_CONFIG, "kafka-ts/cluster1",
            AWS_REGION_CONFIG, "eu-north-1"
        );
        final S3TieredStorageConfig config = new S3TieredStorageConfig(props);
        System.out.println(config);
        final EnvironmentVariableCredentialsProvider provider = new EnvironmentVariableCredentialsProvider();
        final AmazonS3 client = config.s3Client(provider);
        final PutObjectResult result = client.putObject(config.bucketName(), "test", "test");
        System.out.println(result.getMetadata());
    }
}
