package org.apache.flink.streaming.examples.rescale;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.*;

import java.util.*;

public class KafkaProducerWordCount {

    static class RecordGenerator {
        private Random rnd = new Random();
        private int domain = 1024;


        public RecordGenerator(int domain) {
            this.domain = domain;
        }

        private String getRndNumber() {
            return String.valueOf(rnd.nextInt(domain));
        }

    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String kafkaServers = params.get("kafkaServers", "slave205:9092");
        String kafkaTopic = params.get("KafkaTopic", "rhino-test");
        int messageCountPerSec = params.getInt("rate", 10);

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        RecordGenerator recordGenerator = new RecordGenerator(1024);
        Timer timer = new Timer();
        timer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        for (int i = 0; i < messageCountPerSec; i++) {
                            producer.send(new ProducerRecord<>(kafkaTopic, System.currentTimeMillis() + ":" + recordGenerator.getRndNumber()));
                        }
                    }
                },
                5000, 1000
        );
    }
}
