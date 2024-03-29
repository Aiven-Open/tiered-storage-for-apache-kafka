/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.tieredstorage.storage.s3;

import java.util.Objects;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkHttpClientBuilder;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.utils.AttributeMap;

class S3ClientBuilder {
    static S3Client build(final S3StorageConfig config) {
        final software.amazon.awssdk.services.s3.S3ClientBuilder s3ClientBuilder = S3Client.builder();
        final Region region = config.region();
        if (Objects.isNull(config.s3ServiceEndpoint())) {
            s3ClientBuilder.region(region);
        } else {
            s3ClientBuilder.region(region)
                .endpointOverride(config.s3ServiceEndpoint());
        }
        if (config.pathStyleAccessEnabled() != null) {
            s3ClientBuilder.forcePathStyle(config.pathStyleAccessEnabled());
        }

        if (!config.certificateCheckEnabled()) {
            s3ClientBuilder.httpClient(
                new DefaultSdkHttpClientBuilder()
                    .buildWithDefaults(
                        AttributeMap.builder()
                            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                            .build()
                    )
            );
        }

        s3ClientBuilder.serviceConfiguration(builder ->
            builder.checksumValidationEnabled(config.checksumCheckEnabled()));

        final AwsCredentialsProvider credentialsProvider = config.credentialsProvider();
        if (credentialsProvider != null) {
            s3ClientBuilder.credentialsProvider(credentialsProvider);
        }
        s3ClientBuilder.overrideConfiguration(c -> {
            c.addMetricPublisher(new MetricCollector());
            c.apiCallTimeout(config.apiCallTimeout());
            c.apiCallAttemptTimeout(config.apiCallAttemptTimeout());
        });
        return s3ClientBuilder.build();
    }
}
