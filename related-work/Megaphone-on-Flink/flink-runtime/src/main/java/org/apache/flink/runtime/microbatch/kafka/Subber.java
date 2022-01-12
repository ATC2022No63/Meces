package org.apache.flink.runtime.microbatch.kafka;

import org.apache.flink.runtime.microbatch.ProcessConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Subber {
    final private KafkaConsumer<String, String> requestConsumer;
    static int index = 0;
    public static final String rescale_group = "rescale";
    ConsumerRecords<String, String> prev;

    public Subber(String groupid, String host, String topic, String timeout) {
        Properties props = new Properties();
        props.put("group.id", groupid + (index++) + System.nanoTime());
        props.put("bootstrap.servers", host);
        props.put("session.timeout.ms", timeout);
        props.put("max.poll.interval.ms", timeout);
        String offset = groupid.equals(rescale_group) ? "earliest" : "latest";
        props.put("auto.offset.reset", offset);
        this.requestConsumer = new KafkaConsumer<String, String>(props, new StringDeserializer(), new StringDeserializer());
        this.requestConsumer.subscribe(Collections.singleton(topic));
        prev = requestConsumer.poll(0);
    }

    public Subber() {
        this(rescale_group, ProcessConf.getProperty(ProcessConf.KAFKA_HOST),
                ProcessConf.getProperty(ProcessConf.SYNC_TOPIC), ProcessConf.getProperty(ProcessConf.KAFKA_TIMEOUT));
    }

    public ConsumerRecords<String, String> get() {
        if (prev != null) {
            ConsumerRecords<String, String> toReturn = prev;
            prev = null;
            return toReturn;
        }

        while (true) {
            ConsumerRecords<String, String> records = requestConsumer.poll(Duration.ofMillis(1000));
            if (records.count() > 0) {
                return records;
            }
        }
    }

}
