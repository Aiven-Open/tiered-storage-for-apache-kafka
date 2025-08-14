# Getting started with the Iceberg mode

You will need the following tools:
- JDK version 17 or newer;
- Docker;
- Make.

Build the plugin code:
```shell
make plugin
```

Run the Docker Compose:
```shell
docker compose -f docker-compose.yml up
```

Wait until all the containers have started.

Run the demo code:
```shell
clients/gradlew run -p clients
```

It will create the `people` topic and fill it with some Avro records.

Wait until the broker uploads some segments to the remote storage.

Now you can query the Iceberg table using the [Spark notebook](http://localhost:8888/notebooks/notebooks/Demo.ipynb) and observe uploaded files in [Minio](http://localhost:9001/browser/warehouse).
