package org.apache.flink.streaming.examples.rescale;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.PostFetchValueStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * NexmarkQ4.
 */
public class NexmarkQ4 {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		int counterPar = Integer.parseInt(params.get("opPar", "1"));
		int sinkInterval = Integer.parseInt(params.get("sinkInterval", "1000"));
		boolean nativeFlink = Boolean.parseBoolean(params.get("nativeFlink", "true"));

		// env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// kafka consumer
		FlinkKafkaConsumer<String> auctionConsumer = Utils.getConsumer(params, "auctionTopic", "auctionTopic");

		env
			.addSource(auctionConsumer)
			.flatMap(new Utils.KafkaAuctionParser())
			.keyBy(auction -> auction.category)
			.flatMap(new AvgFlatMap(nativeFlink)).setParallelism(counterPar).name("avg")
//			.setMaxParallelism(32)
			.rebalance()
			.addSink(new Utils.SelectiveStringPrintSink(sinkInterval)).name("Sink");

		env.execute("Nexmark q4");
	}

	/**
	 * AvgFlatMap.
	 */
	public static class AvgFlatMap extends RichFlatMapFunction<RescaleStructAuction, String> {

		private ValueState<Tuple2<Long, Long>> sumAndCountState;
		private boolean nativeFlink;

		AvgFlatMap(boolean nativeFlink) {
			super();
			this.nativeFlink = nativeFlink;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			// 创建 ValueStateDescriptor
			ValueStateDescriptor<Tuple2<Long, Long>> sumAndCountStateDescriptor;
			if (!nativeFlink) {
				sumAndCountStateDescriptor =
					new PostFetchValueStateDescriptor<>(
						"sumAndCountStateDescriptor",
						TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
						}));
			} else {
				sumAndCountStateDescriptor =
					new ValueStateDescriptor<Tuple2<Long, Long>>(
						"sumAndCountStateDescriptor",
						TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
						}));
			}

			sumAndCountState = getRuntimeContext().getState(sumAndCountStateDescriptor);
		}

		@Override
		public void flatMap(RescaleStructAuction input, Collector<String> collector) throws Exception {

			Tuple2<Long, Long> sumAndCount = sumAndCountState.value();

			if (null == sumAndCount) {
				sumAndCount = new Tuple2<>(0L, 0L);
			}

			sumAndCount.f0 = input.price + sumAndCount.f0;
			sumAndCount.f1 = 1 + sumAndCount.f1;

			sumAndCountState.update(sumAndCount);

//			collector.collect(String.valueOf(sumAndCount.f0 / sumAndCount.f1));
			collector.collect(input.timestamp + ":" + System.currentTimeMillis());
		}
	}
}
