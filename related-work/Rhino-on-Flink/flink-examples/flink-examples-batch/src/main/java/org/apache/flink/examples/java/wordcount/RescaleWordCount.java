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

package org.apache.flink.examples.java.wordcount;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Implements the "WordCount" program with rescaling states.
 */
public class RescaleWordCount {


    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        long counterCostlyOperationLoops = Long.parseLong(params.get("counterCostlyOperationLoops", "0"));
        String kafkaServers = params.get("kafkaServers", "slave205:9092");
        int counterPar = Integer.parseInt(params.get("counterPar", "1"));
        int defaultPar = Integer.parseInt(params.get("defaultPar", "1"));
        int sinkInterval = Integer.parseInt(params.get("sinkInterval", "1000"));
        int counterMaxPara = Integer.parseInt(params.get("counterMaxPara", "5120"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaServers);
        props.setProperty("group.id", "flink-group");
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(params.get("topic", "rhino-test"), new SimpleStringSchema(), props);


        DataStream<Tuple3<Integer, Integer, Long>> dataStream;

        dataStream = env
                .addSource(consumer)
                .flatMap(new KafkaSplitter()).name("FlatMap-Splitter").setParallelism(defaultPar)
                .keyBy(value -> value.f0)
                .flatMap(new Counter(counterCostlyOperationLoops)).name("FlatMap-Counter")
                .setMaxParallelism(counterMaxPara).setParallelism(counterPar)
        ;

        dataStream
                .flatMap(new TimestampAppenderFlatMap()).setParallelism(counterPar).name("Appender")
                .addSink(new SelectivePrintSink(sinkInterval)).setParallelism(counterPar).name("Sink");

        env.execute("Test WordCount");
    }

    /**
     * Splitter for "WordCount".
     */
    public static class Splitter extends RichFlatMapFunction<Tuple2<String, Long>, Tuple3<String, Integer, String>> {
        @Override
        public void flatMap(Tuple2<String, Long> input, Collector<Tuple3<String, Integer, String>> out) throws Exception {
            for (String word : input.f0.split(" ")) {
                out.collect(new Tuple3<>(word, 1, input.f1 + "-Splitter-" + getRuntimeContext().getIndexOfThisSubtask()));
            }
        }
    }

    /**
     * Splitter for "WordCount" with kafka consumer.
     */
    public static class KafkaSplitter extends RichFlatMapFunction<String, Tuple3<Integer, Integer, Long>> {
        @Override
        public void open(Configuration parameters) {
        }

        @Override
        public void flatMap(String input, Collector<Tuple3<Integer, Integer, Long>> out) {
            String[] content = input.split(":");
            long timestamp = Long.parseLong(content[0]);
            int number = Integer.parseInt(content[1]);
            out.collect(new Tuple3<>(number, 1, timestamp));
        }
    }

    /**
     * WordCountFlatMap for word counting.
     */
    public static class Counter extends RichFlatMapFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>> {

        private ValueState<Integer> valueState;
        private long costlyOperationLoops;

        Counter(long costlyOperationLoops) {
            super();
            this.costlyOperationLoops = costlyOperationLoops;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // 创建 ValueStateDescriptor
            ValueStateDescriptor<Integer> descriptor;
            descriptor =
                    new ValueStateDescriptor<>(
                            "wordCountStateDesc",
                            TypeInformation.of(new TypeHint<Integer>() {
                            }));


            // 基于 ValueStateDescriptor 创建 ValueState
            valueState = getRuntimeContext().getState(descriptor);
            System.out.println(costlyOperationLoops);

        }

        @Override
        public void flatMap(Tuple3<Integer, Integer, Long> input, Collector<Tuple3<Integer, Integer, Long>> collector) throws Exception {
            Integer currentState = valueState.value();

            // 初始化 ValueState 值
            if (null == currentState) {
                currentState = 0;
            }

            // 更新 ValueState 值
            input.f1 = input.f1 + currentState;
            valueState.update(input.f1);

            // make it costly
            for (int i = 0; i < costlyOperationLoops; i++) {
                costlyOperation();
            }

            collector.collect(input);
        }

        private int costlyOperation()  {
            int res = 1;
            for (int i = 1; i < 65536; i++) {
                res *= i;
            }
            return res;
        }
    }

    /**
     * WordCountFlatMap for word counting.
     */
    public static class TimestampAppenderFlatMap extends RichFlatMapFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>> {

        boolean shouldOutput = true;

        @Override
        public void open(Configuration parameters) {

        }

        @Override
        public void flatMap(Tuple3<Integer, Integer, Long> input, Collector<Tuple3<Integer, Integer, Long>> collector) {
            if (shouldOutput) {
                long now = System.currentTimeMillis();
                input.f2 = now - input.f2;
                collector.collect(input);
            }
        }
    }

    /**
     * SelectivePrintSink for word counting.
     */
    public static class SelectivePrintSink extends RichSinkFunction<Tuple3<Integer, Integer, Long>> {
        private boolean shouldOutput = false;
        private final long outputInterval;

        SelectivePrintSink(long outputInterval) {
            this.outputInterval = outputInterval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Timer timer = new Timer();
            timer.schedule(
                    new TimerTask() {
                        @Override
                        public void run() {
                            shouldOutput = true;
                        }
                    }, 0L, outputInterval);
        }

        @Override
        public void invoke(Tuple3<Integer, Integer, Long> value, Context context) {
            if (shouldOutput) {
                System.out.println(value);
                shouldOutput = false;
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}

