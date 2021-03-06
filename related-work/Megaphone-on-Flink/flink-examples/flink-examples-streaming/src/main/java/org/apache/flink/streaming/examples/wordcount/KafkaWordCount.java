/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Implements the "WordCount" program with rescaling states.
 */
public class KafkaWordCount {


    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        long counterCostlyOperationLoops = Long.parseLong(params.get("counterCostlyOperationLoops", "10000"));
        String kafkaServers = params.get("kafkaServers", "slave205:9092");

        int counterPar = Integer.parseInt(params.get("counterPar", "1"));
        int defaultPar = Integer.parseInt(params.get("defaultPar", "1"));
        int sinkInterval = Integer.parseInt(params.get("sinkInterval", "1000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.printf("job init with sinkintervel:%d, loops:%d\n", sinkInterval, counterCostlyOperationLoops);
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaServers);
        props.setProperty("group.id", "flink-group");
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(params.get("topic", "megaphone-test3"), new SimpleStringSchema(), props);
        consumer.setStartFromLatest();

        DataStream<Tuple3<Long, Integer, Long>> dataStream;

        dataStream = env
                .addSource(consumer)
                .flatMap(new KafkaSplitter(sinkInterval)).name("FlatMap-Splitter").setParallelism(defaultPar)
                .keyBy(value -> value.f0)
                .flatMap(new Counter(counterCostlyOperationLoops)).name("FlatMap-Counter")
                .setParallelism(counterPar)
        ;

        dataStream
                .addSink(new SelectivePrintSink(sinkInterval)).setParallelism(1).name("Sink");
        env.enableCheckpointing(Integer.MAX_VALUE);
        env.disableOperatorChaining();
        env.execute("Test WordCount");
    }


    /**
     * Splitter for "WordCount" with kafka consumer.
     */
    static class KafkaSplitter extends RichFlatMapFunction<String, Tuple3<Long, Integer, Long>> {
        long outputInterval;
        boolean print;
        boolean is_task0;

        public KafkaSplitter(long outputInterval) {
            this.outputInterval = outputInterval;
        }

        @Override
        public void open(Configuration parameters) {
        }

        @Override
        public void flatMap(String input, Collector<Tuple3<Long, Integer, Long>> out) {
            String[] content = input.split(":");
            long timestamp = Long.parseLong(content[0]);
            long number = Long.parseLong(content[1]);
            out.collect(new Tuple3<>(number, 1, timestamp));
        }
    }

    /**
     * WordCountFlatMap for word counting.
     */
    static class Counter extends RichFlatMapFunction<Tuple3<Long, Integer, Long>, Tuple3<Long, Integer, Long>> {

        private ValueState<Integer> valueState;
        private long loops;
        private long start;
        public double counter = 0;

        Counter(long costlyOperationLoops) {
            super();
            loops = costlyOperationLoops;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // ?????? ValueStateDescriptor
            ValueStateDescriptor<Integer> descriptor;
            descriptor =
                    new ValueStateDescriptor<>(
                            "wordCountStateDesc",
                            TypeInformation.of(new TypeHint<Integer>() {
                            }));


            // ?????? ValueStateDescriptor ?????? ValueState
            valueState = getRuntimeContext().getState(descriptor);
            start = System.currentTimeMillis();
        }

        @Override
        public void flatMap(Tuple3<Long, Integer, Long> input, Collector<Tuple3<Long, Integer, Long>> collector) throws Exception {
            Integer currentState = valueState.value();

            // ????????? ValueState ???
            if (null == currentState) {
                currentState = 0;
            }
            // ?????? ValueState ???
            input.f1 = input.f1 + currentState;
            valueState.update(input.f1);
            collector.collect(input);
            costlyOperation();
        }

        private void costlyOperation() {
            Double num = 1e8d;
            for (int i = 1; i < loops; i++) {
                num /= i;
                num += i;
            }
            counter += num;
        }
    }

    /**
     * WordCountFlatMap for word counting.
     */
//    static class TimestampAppenderFlatMap extends RichFlatMapFunction<Tuple3<Long, Integer, Long>, Tuple3<Long, Integer, Long>> {
//
//        boolean shouldOutput = true;
//
//        @Override
//        public void open(Configuration parameters) {
//
//        }
//
//        @Override
//        public void flatMap(Tuple3<Long, Integer, Long> input, Collector<Tuple3<Long, Integer, Long>> collector) {
//            if (shouldOutput) {
//                long now = System.currentTimeMillis();
//                input.f2 = now - input.f2;
//                collector.collect(input);
//            }
//        }
//    }

    /**
     * SelectivePrintSink for word counting.
     */
    static class SelectivePrintSink extends RichSinkFunction<Tuple3<Long, Integer, Long>> {
        private boolean shouldOutput = false;
        private final long outputInterval;
        long start;
        long first_ts;
        boolean print = true;
        boolean first = true;

        SelectivePrintSink(long outputInterval) {
            this.outputInterval = outputInterval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            start = System.currentTimeMillis();
            Timer timer = new Timer();
            timer.schedule(
                    new TimerTask() {
                        @Override
                        public void run() {
                            print = true;
                        }
                    }, 0L, outputInterval);
        }

        @Override
        public void invoke(Tuple3<Long, Integer, Long> value, Context context) {
            if (first) {
                first_ts = value.f2;
                first = false;
            }
            if (print ) {
                long now = System.currentTimeMillis();
                System.out.printf("%d:%d:%d:%d:%d\n", now, value.f2, value.f2 - first_ts, now - start, now - value.f2);
//                System.out.printf("%d\n", now - value.f2);

                print = false;
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}


