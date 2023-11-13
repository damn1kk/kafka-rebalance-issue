import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Testcontainers
public class KafkaRebalanceTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaRebalanceTest.class);
    private static final String TOPIC_NAME = "test_topic";
    private static final String TOPIC_RESULT1 = "topic_result1";
    private static final String TOPIC_RESULT2 = "topic_result2";

    private static final Duration REBALANCE_LIMIT = Duration.ofSeconds(30);

    private static final CountDownLatch latch = new CountDownLatch(1);

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));


    @Test
    public void testExample() throws ExecutionException, InterruptedException {
        createTopics();

        KafkaSender<String, String> sender = createSender(false);
        sendValuesToTopic(sender);

        createReceiver("First");
        latch.await();
        createReceiver("Second");

        Thread.sleep(100_000);
    }

    private static void createReceiver(String name) {
        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(
                        Map.of(
                                ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString(),
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                                ConsumerConfig.GROUP_ID_CONFIG, "consumer-group",
                                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10,
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
                        )
                )
                .addAssignListener(onAssign -> log.info("For {} assigned: {}", name, onAssign))
                .addRevokeListener(onRevoke -> log.info("For {} revoked: {}", name, onRevoke))
                .maxDeferredCommits(100)
                .maxDelayRebalance(REBALANCE_LIMIT)
                .subscription(List.of(TOPIC_NAME));


        KafkaSender<String, String> innerSender = createSender(true);
        KafkaReceiver.create(receiverOptions)
                .receiveExactlyOnce(innerSender.transactionManager())
                .concatMap(r -> r.groupBy(ConsumerRecord::partition, 1)
                                .flatMap(c -> innerSender.send(
                                        c.map(cr -> {
                                                    try {
                                                        log.info("Handle message by {} with key: {}", name, Integer.parseInt(cr.key()));
                                                        latch.countDown();
                                                        Thread.sleep(1000);
                                                    } catch (InterruptedException e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                    if (Integer.parseInt(cr.key()) % 2 == 0) {
                                                        return new ProducerRecord<>(TOPIC_RESULT1, cr.key(), cr.value());
                                                    } else {
                                                        return new ProducerRecord<>(TOPIC_RESULT2, cr.key(), cr.value());
                                                    }
                                                })
                                                .map(pr -> SenderRecord.create(pr, pr.key()))

                                ))
                                .then(innerSender.transactionManager().commit()),
                        0
                )
                .subscribe();
    }

    private static void sendValuesToTopic(KafkaSender<String, String> sender) {
        Flux<SenderRecord<String, String, String>> eventFlux = Flux.interval(Duration.ofMillis(100))
                .map(i -> new ProducerRecord<>(TOPIC_NAME, i.toString(), i.toString()))
                .map(pr -> SenderRecord.create(pr, pr.key()));

        sender.send(eventFlux)
//                .doOnNext(r -> log.info("Send message to topic: {}", r.recordMetadata()))
                .subscribe();
    }

    private static void createTopics() throws InterruptedException, ExecutionException {
        Admin admin = Admin.create(
                Map.of(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
                )
        );
        admin.createTopics(Stream.of(TOPIC_NAME, TOPIC_RESULT1, TOPIC_RESULT2)
                .map(t -> new NewTopic(t, 3, (short) 1))
                .collect(Collectors.toList())
        ).all().get();
    }

    private static KafkaSender<String, String> createSender(final boolean isTransactional) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        if (isTransactional) {
            configMap.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
        }
        return KafkaSender.create(SenderOptions.create(configMap));
    }
}
