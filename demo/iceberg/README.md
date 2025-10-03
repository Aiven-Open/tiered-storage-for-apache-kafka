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

Now you can explore and query the Iceberg table using the [Nimtable](http://localhost:3000/data/tables/table?catalog=rest&namespace=default&table=people) (admin:admin) and observe uploaded files in [Minio](http://localhost:9001/browser/warehouse).

```sql
SELECT * 
  FROM `rest`.`default`.`people`
 LIMIT 100
```
