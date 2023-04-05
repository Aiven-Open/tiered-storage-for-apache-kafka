# Docker playground

Docker compose environment to try out tiered storage plugins.

> NOTE: Consider this playground environments unstable until future releases.

## Structure

- A [docker image](./kafka/Dockerfile) is built from a Kafka repository branch
- Each plugin will have a Docker compose, e.g. [S3](./s3/compose.yml), to start an environment with Kafka, Zookeeper, plugin setup, and dependencies.

## How to use

### Test Local Storage

Using test implementation from https://github.com/aiven/kafka/blob/93ae220db12e8294b2cfe8871a2583b53de8b775/storage/src/test/java/org/apache/kafka/server/log/remote/storage/LocalTieredStorage.java

> on [./test](./test) directory

```shell
docker-compose build
# docker-compose build --no-cache # to build again from latest commits
docker-compose up -d
```

Plugin is available on Kafka storage test jars, so this flag is required:

```yaml
kafka:
  # ...
  environment:
    - INCLUDE_TEST_JARS=true
```

Local directory is mounted:

```yaml
kafka:
  # ...
  volumes:
    # mount local dir for second tier
    - ./data:/kafka/kafka-tiered-storage/data
  command:
    # ...
    # Tiered storage S3 plugin
    - --override
    - rsm.config.dir=/data
```

> Internally, `LocalTieredStorage` maps config to internal directory for tiered-storage, yielding `/kafka/kafka-tiered-storage/data`

### S3

> on [./s3](./s3) directory

Build plugin before starting Docker compose:

On root directory:

```shell
./gradlew clean s3:installDist
```

`compose.yml` is mounting the distribution directory:

```yaml
kafka:
  # ...
  volumes:
    - ./../../s3/build/install/s3:/kafka/plugins/tiered-storage-s3
```

and then:

```shell
docker-compose build
# docker-compose build --no-cache # to build again from latest commits
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
    volumes:
      # ...
      - ./private.pem:/kafka/plugins/private.pem
      - ./public.pem:/kafka/plugins/public.pem
    command:
      - kafka-server-start.sh
      - config/server.properties
      # ...
      - --override
      - rsm.config.s3.client.aws_access_key_id=${AWS_ACCESS_KEY_ID}
      - --override
      - rsm.config.s3.client.aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}
```

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
      - rsm.config.s3.public_key_pem=/kafka/plugins/public.pem
      - --override
      - rsm.config.s3.private_key_pem=/kafka/plugins/private.pem
      # ...
```


### Kafka

Creating topics with Tiered storage:

> on current directory

```shell
make ts-topic
```

creates by default a topic `t1` with 6 partitions, `10MB` segments, retention bytes `500MB`, and `50MB` local retention.

```
|-----...----------| partition: 500M
         |-------|-| local: 50M
 |-|-|-|-|-|-|-|-|-| segments: 10M
```

Use Makefile variables to customize topic creation:

```shell
make ts-topic t=t2 segment=1000000
```
