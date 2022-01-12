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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.examples.join.WindowJoinSampleData;
import org.apache.flink.streaming.examples.utils.ThrottledIterator;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram over text
 * files in a streaming fashion.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 *
 * <p>This example shows how to:
 *
 * <ul>
 *   <li>write a simple Flink Streaming program,
 *   <li>use tuple data types,
 *   <li>write and use user-defined functions.
 * </ul>
 */
public class WordCount {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        final int p = params.getInt("parallel", 3);
        final int rate = params.getInt("rate", 3);
        final int domain = params.getInt("domain", 32);

        System.out.printf("using p=%d, rate=%d, domain=%d\n", p, rate, domain);
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(p);

        Words.getSource(env, rate, domain, p)
                .keyBy(value -> value.f0)
                .flatMap(new WordCountFlatMap()).name("wordcount").addSink(new DiscardingSink<>()).setParallelism(1);
        env.enableCheckpointing(Integer.MAX_VALUE);
        env.execute("world.execute(me)");
    }


    public static class Words implements Iterator<Tuple2<String, Integer>>, Serializable {

        private final Random rnd = new Random(hashCode());
        private int domain;

        public Words(int domain) {
            this.domain = domain;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple2<String, Integer> next() {
            int r = rnd.nextInt(domain);
            return new Tuple2<>(String.valueOf(r), 1);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public static DataStream<Tuple2<String, Integer>> getSource(
                StreamExecutionEnvironment env, int rate, int domain, int p) {
            return env.fromCollection(
                    new ThrottledIterator<>(new Words(domain), rate),
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                    }));
        }
    }

    public static class WordCountFlatMap extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private ValueState<Integer> valueState;

        WordCountFlatMap() {
            super();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>(
                            "wordCount",
                            TypeInformation.of(new TypeHint<Integer>() {
                            }));

            // 基于 ValueStateDescriptor 创建 ValueState
            valueState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<String, Integer> input, Collector<Tuple2<String, Integer>> collector) throws Exception {
            Integer currentState = valueState.value();

            // 初始化 ValueState 值
            if (null == currentState) {
                currentState = 0;
            }

            Integer newState = input.f1 + currentState;

            // 更新 ValueState 值
            valueState.update(newState);
            input.f1 = newState;
            collector.collect(input);
        }
    }
// *************************************************************************
// USER FUNCTIONS
// *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
