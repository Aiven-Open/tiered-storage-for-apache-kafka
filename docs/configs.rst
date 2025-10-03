=================
Core components
=================
.. Generated from *Config.java classes by io.aiven.kafka.tieredstorage.misc.ConfigsDocs

-----------------
RemoteStorageManagerConfig
-----------------
``chunk.size``
  Segment files are chunked into smaller parts to allow for faster processing (e.g. encryption, compression) and for range-fetching. It is recommended to benchmark this value, starting with 4MiB.

  * Type: int
  * Valid Values: [1,...,1073741823]
  * Importance: high

``storage.backend.class``
  The storage backend implementation class

  * Type: class
  * Importance: high

``compression.enabled``
  Segments can be further compressed to optimize storage usage. Disabled by default.

  * Type: boolean
  * Default: false
  * Importance: high

``compression.heuristic.enabled``
  Only compress segments where native compression has not been enabled. This is currently validated by looking into the first batch header. Only enabled if compression.enabled is enabled.

  * Type: boolean
  * Default: false
  * Importance: high

``encryption.enabled``
  Segments and indexes can be encrypted, so objects are not accessible by accessing the remote storage. Disabled by default.

  * Type: boolean
  * Default: false
  * Importance: high

``key.prefix``
  The object storage path prefix

  * Type: string
  * Default: ""
  * Valid Values: non-null string
  * Importance: high

``iceberg.catalog.class``
  The Iceberg catalog implementation class

  * Type: class
  * Default: null
  * Importance: medium

``iceberg.namespace``
  The Iceberg namespace

  * Type: string
  * Default: null
  * Importance: medium

``segment.format``
  The format of the segment

  * Type: string
  * Default: kafka
  * Valid Values: [kafka, iceberg]
  * Importance: medium

``structure.provider.class``
  The structure provider implementation class

  * Type: class
  * Default: null
  * Importance: medium

``upload.rate.limit.bytes.per.second``
  Upper bound on bytes to upload (therefore read from disk) per second. Rate limit must be equal or larger than 1 MiB/sec as minimal upload throughput.

  * Type: int
  * Default: null
  * Valid Values: null or [1048576,...]
  * Importance: medium

``custom.metadata.fields.include``
  Custom Metadata to be stored along Remote Log Segment metadata on Remote Log Metadata Manager back-end. Allowed values: [REMOTE_SIZE, OBJECT_PREFIX, OBJECT_KEY]

  * Type: list
  * Default: ""
  * Valid Values: [REMOTE_SIZE, OBJECT_PREFIX, OBJECT_KEY]
  * Importance: low

``key.prefix.mask``
  Whether to mask path prefix in logs

  * Type: boolean
  * Default: false
  * Importance: low

``metrics.num.samples``
  The number of samples maintained to compute metrics.

  * Type: int
  * Default: 2
  * Valid Values: [1,...]
  * Importance: low

``metrics.recording.level``
  The highest recording level for metrics.

  * Type: string
  * Default: INFO
  * Valid Values: [INFO, DEBUG, TRACE]
  * Importance: low

``metrics.sample.window.ms``
  The window of time a metrics sample is computed over.

  * Type: long
  * Default: 30000 (30 seconds)
  * Valid Values: [1,...]
  * Importance: low



-----------------
SegmentManifestCacheConfig
-----------------
Under ``fetch.manifest.cache.``

``retention.ms``
  Cache retention time ms, where "-1" represents infinite retention

  * Type: long
  * Default: 3600000 (1 hour)
  * Valid Values: [-1,...,9223372036854775807]
  * Importance: medium

``size``
  The maximum number of entries in the cache, where `-1` represents an unbounded cache.

  * Type: long
  * Default: 1000
  * Valid Values: [-1,...,9223372036854775807]
  * Importance: medium

``get.timeout.ms``
  When getting an object from the fetch, how long to wait before timing out. Defaults to 10 sec.

  * Type: long
  * Default: 10000 (10 seconds)
  * Valid Values: [1,...,9223372036854775807]
  * Importance: low

``thread.pool.size``
  Size for the thread pool used to schedule asynchronous fetching tasks, default to number of processors.

  * Type: int
  * Default: 0
  * Valid Values: [0,...,1024]
  * Importance: low



-----------------
SegmentIndexesCacheConfig
-----------------
Under ``fetch.indexes.cache.``

``retention.ms``
  Cache retention time ms, where "-1" represents infinite retention

  * Type: long
  * Default: 600000 (10 minutes)
  * Valid Values: [-1,...,9223372036854775807]
  * Importance: medium

``size``
  Cache size in bytes, where "-1" represents unbounded cache

  * Type: long
  * Default: 10485760
  * Valid Values: [-1,...,9223372036854775807]
  * Importance: medium

``get.timeout.ms``
  When getting an object from the fetch, how long to wait before timing out. Defaults to 10 sec.

  * Type: long
  * Default: 10000 (10 seconds)
  * Valid Values: [1,...,9223372036854775807]
  * Importance: low

``thread.pool.size``
  Size for the thread pool used to schedule asynchronous fetching tasks, default to number of processors.

  * Type: int
  * Default: 0
  * Valid Values: [0,...,1024]
  * Importance: low



-----------------
ChunkManagerFactoryConfig
-----------------
``fetch.chunk.cache.class``
  Chunk cache implementation. There are 2 implementations included: io.aiven.kafka.tieredstorage.fetch.cache.MemoryChunkCache and io.aiven.kafka.tieredstorage.fetch.cache.DiskChunkCache

  * Type: class
  * Default: null
  * Valid Values: Any implementation of io.aiven.kafka.tieredstorage.fetch.cache.ChunkCache
  * Importance: medium



-----------------
MemoryChunkCacheConfig
-----------------
Under ``fetch.chunk.cache.``

``size``
  Cache size in bytes, where "-1" represents unbounded cache

  * Type: long
  * Valid Values: [-1,...,9223372036854775807]
  * Importance: medium

``prefetch.max.size``
  The amount of data that should be eagerly prefetched and cached

  * Type: int
  * Default: 0
  * Valid Values: [0,...,2147483647]
  * Importance: medium

``retention.ms``
  Cache retention time ms, where "-1" represents infinite retention

  * Type: long
  * Default: 600000 (10 minutes)
  * Valid Values: [-1,...,9223372036854775807]
  * Importance: medium

``get.timeout.ms``
  When getting an object from the fetch, how long to wait before timing out. Defaults to 10 sec.

  * Type: long
  * Default: 10000 (10 seconds)
  * Valid Values: [1,...,9223372036854775807]
  * Importance: low

``thread.pool.size``
  Size for the thread pool used to schedule asynchronous fetching tasks, default to number of processors.

  * Type: int
  * Default: 0
  * Valid Values: [0,...,1024]
  * Importance: low



-----------------
DiskChunkCacheConfig
-----------------
Under ``fetch.chunk.cache.``

``path``
  Cache base directory. It is required to exist and be writable prior to the execution of the plugin.

  * Type: string
  * Importance: high

``size``
  Cache size in bytes, where "-1" represents unbounded cache

  * Type: long
  * Valid Values: [-1,...,9223372036854775807]
  * Importance: medium

``prefetch.max.size``
  The amount of data that should be eagerly prefetched and cached

  * Type: int
  * Default: 0
  * Valid Values: [0,...,2147483647]
  * Importance: medium

``retention.ms``
  Cache retention time ms, where "-1" represents infinite retention

  * Type: long
  * Default: 600000 (10 minutes)
  * Valid Values: [-1,...,9223372036854775807]
  * Importance: medium

``get.timeout.ms``
  When getting an object from the fetch, how long to wait before timing out. Defaults to 10 sec.

  * Type: long
  * Default: 10000 (10 seconds)
  * Valid Values: [1,...,9223372036854775807]
  * Importance: low

``thread.pool.size``
  Size for the thread pool used to schedule asynchronous fetching tasks, default to number of processors.

  * Type: int
  * Default: 0
  * Valid Values: [0,...,1024]
  * Importance: low



=================
Storage Backends
=================
Under ``storage.``

-----------------
AzureBlobStorageStorageConfig
-----------------
``azure.container.name``
  Azure container to store log segments

  * Type: string
  * Valid Values: non-empty string
  * Importance: high

``azure.account.name``
  Azure account name

  * Type: string
  * Default: null
  * Valid Values: null or non-empty string
  * Importance: high

``azure.upload.block.size``
  Size of blocks to use when uploading objects to Azure. The smaller the block size, the more calls to Azure are needed to upload a file; increasing costs. The higher the block size, the more memory is needed to buffer the block.

  * Type: int
  * Default: 26214400
  * Valid Values: [102400,...,2147483647]
  * Importance: high

``azure.account.key``
  Azure account key

  * Type: password
  * Default: null
  * Valid Values: null or Non-empty password text
  * Importance: medium

``azure.connection.string``
  Azure connection string. Cannot be used together with azure.account.name, azure.account.key, and azure.endpoint.url

  * Type: password
  * Default: null
  * Valid Values: null or Non-empty password text
  * Importance: medium

``azure.sas.token``
  Azure SAS token

  * Type: password
  * Default: null
  * Valid Values: null or Non-empty password text
  * Importance: medium

``azure.endpoint.url``
  Custom Azure Blob Storage endpoint URL

  * Type: string
  * Default: null
  * Valid Values: null or Valid URL as defined in rfc2396
  * Importance: low



-----------------
AzureBlobStorageStorageConfig
-----------------
``gcs.bucket.name``
  GCS bucket to store log segments

  * Type: string
  * Valid Values: non-empty string
  * Importance: high

``gcs.credentials.default``
  Use the default GCP credentials. Cannot be set together with "gcs.credentials.json" or "gcs.credentials.path"

  * Type: boolean
  * Default: null
  * Importance: medium

``gcs.credentials.json``
  GCP credentials as a JSON string. Cannot be set together with "gcs.credentials.path" or "gcs.credentials.default"

  * Type: password
  * Default: null
  * Valid Values: Non-empty password text
  * Importance: medium

``gcs.credentials.path``
  The path to a GCP credentials file. Cannot be set together with "gcs.credentials.json" or "gcs.credentials.default"

  * Type: string
  * Default: null
  * Valid Values: non-empty string
  * Importance: medium

``gcs.resumable.upload.chunk.size``
  The chunk size for resumable upload. Must be a multiple of 256 KiB (256 x 1024 bytes). Larger chunk sizes typically make uploads faster, but requires bigger memory buffers. The recommended minimum for GCS is 8 MiB. The SDK default is 15 MiB, `see <https://cloud.google.com/storage/docs/resumable-uploads#java>`_. The smaller the chunk size, the more calls to GCS are needed to upload a file; increasing costs. The higher the chunk size, the more memory is needed to buffer the chunk.

  * Type: int
  * Default: 26214400
  * Valid Values: [256 KiB...] values multiple of 262144 bytes
  * Importance: medium

``gcs.endpoint.url``
  Custom GCS endpoint URL. To be used with custom GCS-compatible backends.

  * Type: string
  * Default: null
  * Valid Values: Valid URL as defined in rfc2396
  * Importance: low



-----------------
S3StorageConfig
-----------------
``s3.bucket.name``
  S3 bucket to store log segments

  * Type: string
  * Valid Values: non-empty string
  * Importance: high

``s3.region``
  AWS region where S3 bucket is placed

  * Type: string
  * Importance: medium

``aws.access.key.id``
  AWS access key ID. To be used when static credentials are provided.

  * Type: password
  * Default: null
  * Valid Values: Non-empty password text
  * Importance: medium

``aws.checksum.check.enabled``
  This property is used to enable checksum validation done by AWS library. When set to "false", there will be no validation. It is disabled by default as Kafka already validates integrity of the files.

  * Type: boolean
  * Default: false
  * Importance: medium

``aws.secret.access.key``
  AWS secret access key. To be used when static credentials are provided.

  * Type: password
  * Default: null
  * Valid Values: Non-empty password text
  * Importance: medium

``s3.legacy.md5.plugin.enabled``
  This property is used to enable legacy MD5 plugin. AWS SDK version 2.30.0 introduced integrity protections that are not backward compatible. It is disabled by default since newer version of S3-compatible storages have support for these new integrity protections. It should be enabled when there is a need to access older S3-compatible object storages that depend on the legacy MD5 checksum.

  * Type: boolean
  * Default: false
  * Importance: medium

``s3.multipart.upload.part.size``
  Size of parts in bytes to use when uploading. All parts but the last one will have this size. The smaller the part size, the more calls to S3 are needed to upload a file; increasing costs. The higher the part size, the more memory is needed to buffer the part. Valid values: between 5MiB and 2GiB

  * Type: int
  * Default: 26214400
  * Valid Values: [5242880,...,2147483647]
  * Importance: medium

``aws.certificate.check.enabled``
  This property is used to enable SSL certificate checking for AWS services. When set to "false", the SSL certificate checking for AWS services will be bypassed. Use with caution and always only in a test environment, as disabling certificate lead the storage to be vulnerable to man-in-the-middle attacks.

  * Type: boolean
  * Default: true
  * Importance: low

``aws.credentials.provider.class``
  AWS credentials provider. If not set, AWS SDK uses the default software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain

  * Type: class
  * Default: null
  * Valid Values: Any implementation of software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
  * Importance: low

``s3.api.call.attempt.timeout``
  AWS S3 API call attempt (single retry) timeout in milliseconds

  * Type: long
  * Default: null
  * Valid Values: null or [1,...,9223372036854775807]
  * Importance: low

``s3.api.call.timeout``
  AWS S3 API call timeout in milliseconds, including all retries

  * Type: long
  * Default: null
  * Valid Values: null or [1,...,9223372036854775807]
  * Importance: low

``s3.endpoint.url``
  Custom S3 endpoint URL. To be used with custom S3-compatible backends (e.g. minio).

  * Type: string
  * Default: null
  * Valid Values: Valid URL as defined in rfc2396
  * Importance: low

``s3.path.style.access.enabled``
  Whether to use path style access or virtual hosts. By default, empty value means S3 library will auto-detect. Amazon S3 uses virtual hosts by default (true), but other S3-compatible backends may differ (e.g. minio).

  * Type: boolean
  * Default: null
  * Importance: low

``s3.storage.class``
  Defines which storage class to use when uploading objects

  * Type: string
  * Default: STANDARD
  * Valid Values: [STANDARD, REDUCED_REDUNDANCY, STANDARD_IA, ONEZONE_IA, INTELLIGENT_TIERING, GLACIER, DEEP_ARCHIVE, OUTPOSTS, GLACIER_IR, SNOW, EXPRESS_ONEZONE, FSX_OPENZFS]
  * Importance: low



-----------------
OciStorageConfig
-----------------
``oci.bucket.name``
  oci bucket to store log segments

  * Type: string
  * Valid Values: non-empty string
  * Importance: high

``oci.namespace.name``
  oci namespace which the bucket belongs to

  * Type: string
  * Valid Values: non-empty string
  * Importance: high

``oci.region``
  OCI region where the bucket is placed

  * Type: string
  * Importance: high

``oci.multipart.upload.part.size``
  Size of parts in bytes to use when uploading. All parts but the last one will have this size. The smaller the part size, the more calls to oci are needed to upload a file; increasing costs. The higher the part size, the more memory is needed to buffer the part. Valid values: between 5MiB and 2GiB

  * Type: int
  * Default: 26214400
  * Valid Values: [5242880,...,2147483647]
  * Importance: medium

``oci.storage.tier``
  Defines which storage tier to use when uploading objects

  * Type: string
  * Default: UnknownEnumValue
  * Valid Values: [Standard, InfrequentAccess, Archive, UnknownEnumValue]
  * Importance: medium



-----------------
FilesystemStorageConfig
-----------------
.. Only for development/testing purposes
``root``
  Root directory

  * Type: string
  * Importance: high

``overwrite.enabled``
  Enable overwriting existing files

  * Type: boolean
  * Default: false
  * Importance: medium



