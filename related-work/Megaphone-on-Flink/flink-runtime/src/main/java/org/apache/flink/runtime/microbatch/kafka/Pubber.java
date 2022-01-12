package org.apache.flink.runtime.microbatch.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class Pubber {
    final private KafkaProducer<String, String> requestProducer;
    static int index = 0;
    public static final String BATCH_PREFIX = "batch";
    public static final String MIGRATION_PREFIX = "migration";
    public static final String PUB_PREFIX="pub";

    public Pubber(String host, String timeout) {
        Properties props = new Properties();

        props.put("bootstrap.servers", host);
        props.put("session.timeout.ms", timeout);
        props.put("max.poll.interval.ms", timeout);
        props.put("acks", "0");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        this.requestProducer = new KafkaProducer<>(
                props,
                new StringSerializer(),
                new StringSerializer());

    }

    public void send(String topic, String msg) {
        requestProducer.send(new ProducerRecord<String, String>(topic, "", msg));
        requestProducer.flush();
    }
}
