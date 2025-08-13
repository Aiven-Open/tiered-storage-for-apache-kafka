package io.aiven;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;

import com.arakelian.faker.model.Person;
import com.arakelian.faker.service.RandomPerson;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Clients {
    private static Logger LOG = LoggerFactory.getLogger(Clients.class);

    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String TOPIC_NAME = "people";

    private static Schema VALUE_SCHEMA = SchemaBuilder.record("peopleValue")
        .fields()
        .name("firstName").type().stringType().noDefault()
        .name("lastName").type().stringType().noDefault()
        .name("created").type().stringType().noDefault()
        .name("updated").type().stringType().noDefault()
        .name("title").type().stringType().noDefault()
        .name("age").type().intType().noDefault()
        .name("birthdate").type().stringType().noDefault()
        .name("comments").type().stringType().noDefault()
        .endRecord();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        createTopic();
        produce();
        consume();
    }

    private static void createTopic() throws ExecutionException, InterruptedException {
        try (final AdminClient adminClient = KafkaAdminClient.create(Map.of(
            "bootstrap.servers", BOOTSTRAP_SERVER
        ))) {
            LOG.info("Deleting topic {} (if exists)", TOPIC_NAME);
            try {
                adminClient.deleteTopics(List.of(TOPIC_NAME)).all().get();
            } catch (final ExecutionException | InterruptedException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                    throw e;
                }
            }

            LOG.info("Creating topic {}", TOPIC_NAME);
            final Map<String, String> topicConfig = Map.of(
                "segment.bytes", Integer.toString(1024 * 1024),
                "remote.storage.enable", "true",
                "retention.ms", "360000000",
                "local.retention.ms", "1",
                "retention.bytes", Integer.toString(1024 * 1024 * 1024),
                "local.retention.bytes", "1"
            );
            adminClient.createTopics(List.of(
                new NewTopic(TOPIC_NAME, 1, (short) 1).configs(topicConfig)
            )).all().get();
        }

    }

    private static void produce() {
        try (final KafkaProducer<GenericData.Record, GenericData.Record> producer = new KafkaProducer<>(Map.of(
            "bootstrap.servers", BOOTSTRAP_SERVER,
            "key.serializer", ByteArraySerializer.class.getCanonicalName(),
            "value.serializer", KafkaAvroSerializer.class.getCanonicalName(),
            "schema.registry.url", SCHEMA_REGISTRY_URL
        ))) {

            final RandomPerson randomPerson = RandomPerson.get();
            for (int i = 0; i < 1000; i++) {
                final Person person = randomPerson.next();
                final GenericData.Record value = new GenericRecordBuilder(VALUE_SCHEMA)
                    .set("firstName", person.getFirstName())
                    .set("lastName", person.getLastName())
                    .set("created", person.getCreated().toString())
                    .set("updated", person.getUpdated().toString())
                    .set("title", person.getTitle())
                    .set("age", person.getAge())
                    .set("birthdate", person.getBirthdate().toString())
                    .set("comments", person.getComments())
                    .build();
                producer.send(new ProducerRecord<>(TOPIC_NAME, 0, Time.SYSTEM.milliseconds(), null, value));
            }
            producer.flush();
        }
    }

    private static void consume() {
        try (final KafkaConsumer<GenericData.Record, GenericData.Record> consumer = new KafkaConsumer<>(Map.of(
            "bootstrap.servers", BOOTSTRAP_SERVER,
            "key.deserializer", KafkaAvroDeserializer.class.getCanonicalName(),
            "value.deserializer", KafkaAvroDeserializer.class.getCanonicalName(),
            "schema.registry.url", SCHEMA_REGISTRY_URL
        ))) {
            final TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, 0);
            final ConsumerRecords<GenericData.Record, GenericData.Record> poll = consumer.poll(Duration.ofSeconds(3));
            for (final var record : poll.records(TOPIC_NAME)) {
                System.out.printf("%s%n", record.value());
            }
        }
    }
}
