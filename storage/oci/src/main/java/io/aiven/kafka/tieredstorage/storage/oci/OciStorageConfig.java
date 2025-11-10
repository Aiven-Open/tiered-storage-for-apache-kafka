/*
 * Copyright 2023 Aiven Oy
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

package io.aiven.kafka.tieredstorage.storage.oci;

import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider;
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.model.StorageTier;

public class OciStorageConfig extends AbstractConfig {

    public static final String OCI_NAMESPACE_NAME_CONFIG = "oci.namespace.name";
    private static final String OCI_NAMESPACE_NAME_DOC = "oci namespace which the bucket belongs to";
    public static final String OCI_BUCKET_NAME_CONFIG = "oci.bucket.name";
    private static final String OCI_BUCKET_NAME_DOC = "oci bucket to store log segments";
    public static final String OCI_REGION_CONFIG = "oci.region";
    private static final String OCI_REGION_DOC = "OCI region where the bucket is placed";

    private static final String OCI_MULTIPART_UPLOAD_PART_SIZE_CONFIG = "oci.multipart.upload.part.size";
    private static final String OCI_MULTIPART_UPLOAD_PART_SIZE_DOC = "Size of parts in bytes to use when uploading. "
        + "All parts but the last one will have this size. "
        + "The smaller the part size, the more calls to oci are needed to upload a file; increasing costs. "
        + "The higher the part size, the more memory is needed to buffer the part. "
        + "Valid values: between 5MiB and 2GiB";
    static final int OCI_MULTIPART_UPLOAD_PART_SIZE_MIN = 5 * 1024 * 1024; // 5MiB
    static final int OCI_MULTIPART_UPLOAD_PART_SIZE_MAX = Integer.MAX_VALUE;
    static final int OCI_MULTIPART_UPLOAD_PART_SIZE_DEFAULT = 25 * 1024 * 1024; // 25MiB

    public static final String OCI_STORAGE_TIER_CONFIG = "oci.storage.tier";
    private static final String OCI_STORAGE_TIER_DOC = "Defines which storage tier to use when uploading objects";
    static final String OCI_STORAGE_TIER_CONFIG_DEFAULT = StorageTier.UnknownEnumValue.toString();

    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(
                OCI_NAMESPACE_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                OCI_NAMESPACE_NAME_DOC)
            .define(
                OCI_BUCKET_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                OCI_BUCKET_NAME_DOC)
            .define(
                OCI_REGION_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                OCI_REGION_DOC)
            .define(
                OCI_MULTIPART_UPLOAD_PART_SIZE_CONFIG,
                ConfigDef.Type.INT,
                OCI_MULTIPART_UPLOAD_PART_SIZE_DEFAULT,
                ConfigDef.Range.between(OCI_MULTIPART_UPLOAD_PART_SIZE_MIN, OCI_MULTIPART_UPLOAD_PART_SIZE_MAX),
                ConfigDef.Importance.MEDIUM,
                OCI_MULTIPART_UPLOAD_PART_SIZE_DOC)
            .define(
                OCI_STORAGE_TIER_CONFIG,
                ConfigDef.Type.STRING,
                OCI_STORAGE_TIER_CONFIG_DEFAULT,
                ConfigDef.ValidString.in(
                        Arrays.stream(StorageTier.values()).map(Object::toString).toArray(String[]::new)),
                ConfigDef.Importance.MEDIUM,
                OCI_STORAGE_TIER_DOC);
    }

    public OciStorageConfig(final Map<String, ?> props) {
        super(configDef(), props);
    }

    Region region() {
        return Region.fromRegionId(getString(OCI_REGION_CONFIG));
    }

    AbstractAuthenticationDetailsProvider credentialsProvider() {
        return InstancePrincipalsAuthenticationDetailsProvider.builder().build();
    }

    public String namespaceName() {
        return getString(OCI_NAMESPACE_NAME_CONFIG);
    }

    public String bucketName() {
        return getString(OCI_BUCKET_NAME_CONFIG);
    }

    public StorageTier storageTier(){
        return StorageTier.valueOf(getString(OCI_STORAGE_TIER_CONFIG));
    }

    public int uploadPartSize() {
        return getInt(OCI_MULTIPART_UPLOAD_PART_SIZE_CONFIG);
    }

}
