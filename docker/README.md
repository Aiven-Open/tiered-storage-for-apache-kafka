# Docker playground

Docker compose environment to try out tiered storage plugins.

> NOTE: Consider this playground environments unstable until future releases.

## Structure

- A [docker image](./kafka/Dockerfile) is built from a Kafka repository branch
- Each plugin will have a Docker compose, e.g. [S3](./s3/compose.yml), to start an environment with Kafka, Zookeeper, plugin setup, and dependencies.

## How to use

### Pre-requisites

Build plugin before starting Docker compose:

#### Using Encryption mechanisms

Generate RSA key pair:

```shell
make rsa-keys
```

and set paths on `compose.yml` file:

```yaml
  kafka:
    # ...
    volumes:
      # ...
      - ./private.pem:/kafka/plugins/private.pem
      - ./public.pem:/kafka/plugins/public.pem
    command:
      - kafka-server-start.sh
      - config/server.properties
      # ...
      - --override
      - rsm.config.encryption.public.key.file=/kafka/plugins/public.pem
      - --override
      - rsm.config.encryption.private.key.file=/kafka/plugins/private.pem
      # ...
```

### S3

> for AWS S3 try [./s3](./s3) directory and
> for Minio S3 try [./s3-minio](./s3-minio) directory

Start docker compose (will build image if needed):

```shell
docker-compose up -d
```

#### Connect to AWS S3

Set AWS credentials on `.env` file:

```properties .env
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

and use variables on `compose.yml`:

```yaml
  kafka:
    # ...
    command:
      - kafka-server-start.sh
      - config/server.properties
      # ...
      - --override
      - rsm.config.storage.aws.access.key.id=${AWS_ACCESS_KEY_ID}
      - --override
      - rsm.config.storage.aws.secret.access.key=${AWS_SECRET_ACCESS_KEY}
```

### Kafka

Creating topics with Tiered storage:

```shell
make kafka-topic
```

creates a topic t1 with 6 partitions, 10MB segments, retention bytes 100MB, and 20MB local retention,
and start writing messages to test retention and shipping logs to tiered-storage.
