# Docker playground

Docker compose environment to try out tiered storage plugins.

The project uses a Docker image built from Aiven Kafka fork (e.g. https://github.com/aiven/kafka/tree/3.3-2022-10-06-tiered-storage) and https://github.com/aiven/tiered-storage-for-apache-kafka.

## Requirements

- Docker Compose
- `make`
- `kcat`

## Structure

- S3:
  - s3/compose.yml: Expects environment variables to connect to AWS S3.
  - s3-minio/compose.yml: Starts a local MinIO server to connect via S3 API.
- Local:
  - filesystem/compose.yml: Creates a local temporal directory with Tiered Storage plugin.

## How to use

### Preparation

#### Connection to S3

Set AWS credentials on `.env` file on `./s3` directory:

```properties .env
AWS_S3_BUCKET=test-kafka-ts
AWS_REGION=us-west-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
# ... e.g. AWS_SESSION_TOKEN
```

### Running

- Start docker compose environments by running `make run_kafka_*`
- Create the topic: `make create_topic`.
- Fill the topic: `make fill_topic`.
- Wait for a few seconds for old segment files in `/var/lib/kafka/data/t1-0` to be removed or renamed with `.deleted` suffix.

To check files are shipped to tiered-storage:

- File-system:
  - Run `make check_fs_remote_data` to list data within the target directory.
- S3:
  - Access AWS S3 console or MinIO console (http://localhost:9090) to list and check files on remote storage

And consume data from remote storage via Kafka Consumer:

- Running kcat `make consume` and check previous offsets if needed `make consume args="-p 0 -o 120000"` 

### Additional features

#### Using Encryption mechanisms

Generate RSA key pair:

```shell
make rsa_keys
```

and set paths on `compose.yml` file:

```yaml
  kafka:
    # ...
    volumes:
      # ...
      - ./public.pem:/kafka/plugins/public.pem
      - ./private.pem:/kafka/plugins/private.pem
    environment:
      # ...
      KAFKA_RSM_CONFIG_STORAGE_ENCRYPTION_ENABLED: true
      KAFKA_RSM_CONFIG_STORAGE_ENCRYPTION_PUBLIC_KEY_FILE: /kafka/plugins/public.pem
      KAFKA_RSM_CONFIG_STORAGE_ENCRYPTION_PRIVATE_KEY_FILE: /kafka/plugins/private.pem
      # ...
```
