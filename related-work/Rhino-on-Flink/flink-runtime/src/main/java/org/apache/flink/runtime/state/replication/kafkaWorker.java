package org.apache.flink.runtime.state.replication;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class kafkaWorker {
    private static final Properties confProperties = ProcessLevelConf.getPasaConf();
    public static final String kafkaHost = confProperties.getProperty(ProcessLevelConf.KAFKA_HOST);
    private static final String sessionTimeout = confProperties.getProperty(ProcessLevelConf.KAFKA_TIMEOUT);
    protected static final Logger log = LoggerFactory.getLogger(kafkaWorker.class);

    final private String topic;
    final private KafkaConsumer<String, String> requestConsumer;
    final private KafkaProducer<String, String> requestProducer;
    static Random r = new Random();

    static {
        r.setSeed(System.nanoTime());
    }

    public kafkaWorker(String groupid, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaHost);

        int rr = r.nextInt();
        props.put("group.id", groupid + rr);
        props.put("auto.offset.reset", "latest");
        log.info("timeout:{}", sessionTimeout);
        props.put("session.timeout.ms", sessionTimeout);
        props.put("max.poll.interval.ms", sessionTimeout);
        this.requestConsumer = new KafkaConsumer<String, String>(props, new StringDeserializer(), new StringDeserializer());
        this.requestConsumer.subscribe(Collections.singleton(topic));

        props.put("acks", "0");
        props.put("retries", 0);
        props.put("batch.size", 16384);

        this.requestProducer = new KafkaProducer<>(
                props,
                new StringSerializer(),
                new StringSerializer());
    }

    public void send(String msg) {
        requestProducer.send(new ProducerRecord<String, String>(topic, "", msg));
        requestProducer.flush();
    }

    public ConsumerRecords<String, String> get() {
        while (true) {
            ConsumerRecords<String, String> records = requestConsumer.poll(Duration.ofMillis(1000));
            if (records.count() > 0) {
                for (ConsumerRecord<String, String> record : records) {
                    log.info("get msg {}", record.value());
                }
                return records;
            }
        }
    }

    public void init() {
        requestConsumer.poll(1);
    }

}
