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

package org.apache.flink.streaming.examples.rescale;

import org.apache.commons.lang3.RandomStringUtils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.PostFetchValueStateDescriptor;
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
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Implements the "WordCount" program with rescaling states.
 */
public class RescaleWordCount {
	private static class RandomWordSource implements SourceFunction<Tuple2<String, Long>> {
		private static final long serialVersionUID = 1L;

		private Random rnd = new Random();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		private final String wordFilePath;
		private final int lineIntervalMilliSeconds;
		private final boolean useDictionary;

		private volatile boolean isRunning = true;
		private Map<String, Integer> count = new HashMap<>();

		private RandomWordSource(String wordFilePath, int lineIntervalMilliSeconds, boolean useDictionary) {
			this.wordFilePath = wordFilePath;
			this.lineIntervalMilliSeconds = lineIntervalMilliSeconds;
			this.useDictionary = useDictionary;
		}

		@Override
		public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {

//			if (useDictionary) {
//				runWithDictionary(ctx);
//			} else {
//				runWithoutDictionary(ctx);
//			}
			while (isRunning) {
				ctx.collect(new Tuple2<>(getOneRandomSentence(), System.currentTimeMillis()));
			}

		}

		private String getOneRandomSentence(){
			return RandomStringUtils.random(5, true, false);
		}

		private void runWithDictionary(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(wordFilePath));
			ArrayList<String> wordList = new ArrayList<>();
			String strLine;
			while (null != (strLine = bufferedReader.readLine())){
				if (StringUtils.isBlank(strLine)) {
					continue;
				}
				wordList.add(strLine);
			}
			String[] dictionary = wordList.toArray(new String[0]);

			long now = System.currentTimeMillis();
			while (isRunning && (System.currentTimeMillis() - now <= 200000)) {
				String line = dictionary[rnd.nextInt(dictionary.length)];
				ctx.collect(new Tuple2<>(line, System.currentTimeMillis()));
				for (String str: line.split(" ")) {
					if (count.containsKey(str)) {
						count.put(str, count.get(str) + 1);
					} else {
						count.put(str, 1);
					}
				}
				Thread.sleep(lineIntervalMilliSeconds);
			}
			while (isRunning) {
				Thread.sleep(1000);
			}
		}

		private void runWithoutDictionary(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(wordFilePath));

			while (isRunning) {
				String line = bufferedReader.readLine();
				if (StringUtils.isBlank(line)) {
					continue;
				}
				ctx.collect(new Tuple2<>(line, System.currentTimeMillis()));
				Thread.sleep(lineIntervalMilliSeconds);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
			for (Map.Entry<String, Integer> entry : count.entrySet()) {
				System.out.println(entry.getKey() + ":" + entry.getValue());
			}
		}
	}

	private static class RandomWordSource2 implements ParallelSourceFunction<String> {
		private volatile boolean isRunning = true;
		private int messageCountPerSec;

		RandomWordSource2(int messageCountPerSec) {
			this.messageCountPerSec = messageCountPerSec;
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			if (messageCountPerSec <= 0) {
				while (isRunning) {
					ctx.collect(System.currentTimeMillis() + ":" + getOneRandomSentence());
				}
			} else {
				Timer timer = new Timer();
				timer.schedule(
					new TimerTask() {
						@Override
						public void run() {
							for (int i = 0; i < messageCountPerSec / 5; i++) {
								ctx.collect(
									System.currentTimeMillis() + ":" + getOneRandomSentence());
							}
						}
					},
					0, 200
				);

				while (isRunning) {
					Thread.sleep(1000);
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		private String getOneRandomSentence(){
			return RandomStringUtils.random(8, false, true);
		}
	}

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		String wordFilePath = params.get("wordFile", "test");
		int lineIntervalMilliSeconds = Integer.parseInt(params.get("interval", "500"));
		int sourceRate = Integer.parseInt(params.get("sourceRate", "10000"));
		boolean useDictionary = Boolean.parseBoolean(params.get("useDictionary", "false"));
		long counterCostlyOperationLoops = Long.parseLong(params.get("counterCostlyOperationLoops", "0"));
//        String kafkaHost = params.get("KafkaHost", "localhost");
//        String kafkaPort = params.get("KafkaPort", "9092");
		String kafkaServers = params.get("kafkaServers", "localhost:9092");
		boolean startFromEarliest = Boolean.parseBoolean(params.get("startFromEarliest", "false"));
		int counterPar = Integer.parseInt(params.get("counterPar", "1"));
		int defaultPar = Integer.parseInt(params.get("defaultPar", "1"));
		int sinkInterval = Integer.parseInt(params.get("sinkInterval", "1000"));
		int counterMaxPara = Integer.parseInt(params.get("counterMaxPara", "5120"));
		boolean nativeFlink = Boolean.parseBoolean(params.get("nativeFlink", "true"));

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.disableOperatorChaining();

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", kafkaServers);
		props.setProperty("group.id", "flink-group");
		FlinkKafkaConsumer<String> consumer =
			new FlinkKafkaConsumer<>(params.get("topic", "test"), new SimpleStringSchema(), props);

		if (startFromEarliest) {
			consumer.setStartFromEarliest();
		} else {
			consumer.setStartFromLatest();
		}

		DataStream<Tuple3<Integer, Integer, StringBuilder>> dataStream;
//		DataStream<Tuple3<String, Integer, String>> dataStream = env
//			.addSource(new RandomWordSource(wordFilePath, lineIntervalMilliSeconds, useDictionary))
//			.flatMap(new Splitter()).name("FlatMap-Splitter")
//			.keyBy(value -> value.f0)
//			.flatMap(new WordCountFlatMap(counterIntervalMilliSecs)).name("FlatMap-Counter").setParallelism(counterPar).rebalance();

//		DataStream<Tuple3<String, Integer, String>> dataStream = env
//			.addSource(new RandomWordSource(wordFilePath, lineIntervalMilliSeconds, useDictionary)).rebalance()
//			.flatMap(new Splitter()).name("FlatMap-Splitter").rebalance()
//			.keyBy(value -> value.f0)
//			.flatMap(new WordCountFlatMap(counterIntervalMilliSecs)).name("FlatMap-Counter").rebalance();
//

		dataStream = env
//			.addSource(new RandomWordSource2(sourceRate)).setParallelism(defaultPar)
			.addSource(consumer)
			.flatMap(new KafkaSplitter()).name("FlatMap-Splitter").setParallelism(defaultPar)
			.keyBy(value -> value.f0)
			.flatMap(new WordCountFlatMap(nativeFlink, counterCostlyOperationLoops)).name("FlatMap-Counter")
//			.sum(1)
			.setMaxParallelism(counterMaxPara).setParallelism(counterPar)
//			.rebalance()
			;

		dataStream
			.flatMap(new TimestampAppenderFlatMap()).setParallelism(counterPar).name("Appender")
			.addSink(new SelectivePrintSink(sinkInterval)).setParallelism(counterPar).name("Sink");
//			.print();

		env.execute("Test WordCount");
	}

	/**
	 * Splitter for "WordCount".
	 */
	public static class Splitter extends RichFlatMapFunction<Tuple2<String, Long>, Tuple3<String, Integer, String>> {
		@Override
		public void flatMap(Tuple2<String, Long> input, Collector<Tuple3<String, Integer, String>> out) throws Exception {
			for (String word: input.f0.split(" ")) {
				out.collect(new Tuple3<>(word, 1, input.f1 + "-Splitter-" + getRuntimeContext().getIndexOfThisSubtask()));
			}
		}
	}

	/**
	 * Splitter for "WordCount" with kafka consumer.
	 */
	public static class KafkaSplitter extends RichFlatMapFunction<String, Tuple3<Integer, Integer, StringBuilder>> {
		private boolean shouldAppendTimeStamp = false;
//		StringBuilder
//		private Tuple3<Integer, Integer, Integer> data = new Tuple3<>(0, 0, 0);

		@Override
		public void open(Configuration parameters) {
			try {
				shouldAppendTimeStamp = InetAddress.getLocalHost().getHostName().equals("slave201");
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void flatMap(String input, Collector<Tuple3<Integer, Integer, StringBuilder>> out) {
			String[] content = input.split(":");
//			long timestamp = Long.parseLong(content[0]);
			for (String word: content[1].split(" ")) {
				out.collect(new Tuple3<>(
					Integer.valueOf(word),
					1,
					new StringBuilder(content[0])));
			}
		}
	}

	/**
	 * WordCountFlatMap for word counting.
	 */
	public static class WordCountFlatMap extends RichFlatMapFunction<Tuple3<Integer, Integer, StringBuilder>, Tuple3<Integer, Integer, StringBuilder>> {

		private ValueState<Integer> valueState;
		private long costlyOperationLoops;
		private boolean nativeFlink;

		WordCountFlatMap(boolean nativeFlink, long costlyOperationLoops) {
			super();
			this.costlyOperationLoops = costlyOperationLoops;
			this.nativeFlink = nativeFlink;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			// 配置 StateTTL(TimeToLive)
//			StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.minutes(3))   // 存活时间
//				.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 永远不返回过期的用户数据
//				.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 每次写操作创建和更新时,修改上次访问时间戳
////                    .setTimeCharacteristic(StateTtlConfig.TimeCharacteristic.ProcessingTime) // 目前只支持 ProcessingTime
//				.build();

			// 创建 ValueStateDescriptor
			ValueStateDescriptor<Integer> descriptor;
			if (!nativeFlink) {
				descriptor =
					new PostFetchValueStateDescriptor<>(
							"wordCountStateDesc",
							TypeInformation.of(new TypeHint<Integer>() {
							}));
			} else {
				descriptor =
					new ValueStateDescriptor<>(
							"wordCountStateDesc",
							TypeInformation.of(new TypeHint<Integer>() {
							}));
			}
//			ValueStateDescriptor<Tuple2<String, Integer>> descriptor =
//				new ValueStateDescriptor<Tuple2<String, Integer>>(
//					"wordCountStateDesc",
//					TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

			// 激活 StateTTL
//			descriptor.enableTimeToLive(ttlConfig);

			// 基于 ValueStateDescriptor 创建 ValueState
			valueState = getRuntimeContext().getState(descriptor);
			System.out.println(costlyOperationLoops);

		}

		@Override
		public void flatMap(Tuple3<Integer, Integer, StringBuilder> input, Collector<Tuple3<Integer, Integer, StringBuilder>> collector) throws Exception {
//			input.f2.append("-").append(System.currentTimeMillis());
			Integer currentState = valueState.value();

			// 初始化 ValueState 值
			if (null == currentState) {
				currentState = 0;
			}

			Integer newState = input.f1 + currentState;

			// 更新 ValueState 值
			valueState.update(newState);

			// make it costly
			for (int i = 0; i < costlyOperationLoops; i++) {
				costlyOperation();
			}
//			Thread.sleep(intervalMilliSecs);

			input.f1 = newState;
//			input.f2.append("-").append(System.currentTimeMillis());
			collector.collect(input);
		}

		private void costlyOperation() throws Exception {
			Integer currentState = valueState.value();

			// 初始化 ValueState 值
			if (null == currentState) {
				currentState = 0;
			}

			// 更新 ValueState 值
			valueState.update(currentState);
		}
	}

	/**
	 * WordCountFlatMap for word counting.
	 */
	public static class TimestampAppenderFlatMap extends RichFlatMapFunction<Tuple3<Integer, Integer, StringBuilder>, Tuple3<Integer, Integer, StringBuilder>> {

		boolean shouldOutput = true;

		@Override
		public void open(Configuration parameters) {
//			try {
//				shouldOutput = InetAddress.getLocalHost().getHostName().equals("slave201");
//			} catch (UnknownHostException e) {
//				e.printStackTrace();
//			}
		}

		@Override
		public void flatMap(Tuple3<Integer, Integer, StringBuilder> input, Collector<Tuple3<Integer, Integer, StringBuilder>> collector) {
			if (shouldOutput) {
				input.f2.append("-").append(System.currentTimeMillis());
				collector.collect(input);
			}
		}
	}

	/**
	 * SelectivePrintSink for word counting.
	 */
	public static class SelectivePrintSink extends RichSinkFunction<Tuple3<Integer, Integer, StringBuilder>> {
		private boolean shouldOutput = false;
		private final long outputInterval;

		SelectivePrintSink(long outputInterval) {
			this.outputInterval = outputInterval;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
//			if (getRuntimeContext().getIndexOfThisSubtask() % 10 == 0) {
				Timer timer = new Timer();
				timer.schedule(
					new TimerTask() {
						@Override
						public void run() {
							shouldOutput = true;
						}
					}, 0L, outputInterval);
//			}
		}

		@Override
		public void invoke(Tuple3<Integer, Integer, StringBuilder> value, Context context) {
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

