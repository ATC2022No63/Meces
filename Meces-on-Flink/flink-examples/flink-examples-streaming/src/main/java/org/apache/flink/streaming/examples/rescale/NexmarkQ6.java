package org.apache.flink.streaming.examples.rescale;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.PostFetchValueStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * NexmarkQ6.
 */
public class NexmarkQ6 {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		int windowPar = Integer.parseInt(params.get("opPar", "1"));
		int sinkInterval = Integer.parseInt(params.get("sinkInterval", "1000"));
		final int slidingWindowSize = Integer.parseInt(params.get("windowSize", "5"));
		final int slidingWindowStep = Integer.parseInt(params.get("windowStep", "100"));
		boolean nativeFlink = Boolean.parseBoolean(params.get("nativeFlink", "true"));

		// env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// kafka consumer
		FlinkKafkaConsumer<String> auctionConsumer = Utils.getConsumer(params, "auctionTopic", "auctionTopic");

		env
			.addSource(auctionConsumer)
			.flatMap(new Utils.KafkaAuctionParser())
			.keyBy(auction -> auction.sellerId)
			.flatMap(new AvgFlatMap(nativeFlink))
//			.window(Utils.OutputSlidingProcessingTimeWindows.of(Time.seconds(slidingWindowSize), Time.milliseconds(slidingWindowStep)))
//			.window(TumblingProcessingTimeWindows.of(Time.seconds(slidingWindowSize)))
//			.process(new ProcessWindowFunction<RescaleStructAuction, String, Long, TimeWindow>() {
//				@Override
//				public void process(
//					Long aLong,
//					Context context,
//					Iterable<RescaleStructAuction> elements,
//					Collector<String> out) throws Exception {
//					int sum = 0, count = 0;
//					for (RescaleStructAuction rescaleStructAuction : elements) {
//						sum += rescaleStructAuction == null ? 0 : rescaleStructAuction.price;
//						count++;
//					}
//					out.collect("" + (count == 0 ? 0 : sum / count));
//				}
//			})
			.name("window")
			.setParallelism(windowPar)
			.rebalance()
			.addSink(new Utils.SelectiveStringPrintSink(sinkInterval)).name("Sink");

		env.execute("Nexmark q6");
	}

	/**
	 * AvgFlatMap.
	 */
	public static class AvgFlatMap extends RichFlatMapFunction<RescaleStructAuction, String> {

		// TODO: should use list state and calculate average
		private ValueState<String> recordState;
		private boolean nativeFlink;

		AvgFlatMap(boolean nativeFlink) {
			super();
			this.nativeFlink = nativeFlink;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			// 创建 ValueStateDescriptor
			ValueStateDescriptor<String> sumAndCountStateDescriptor;
			if (!nativeFlink) {
				sumAndCountStateDescriptor =
					new PostFetchValueStateDescriptor<>(
						"recordStateDescriptor",
						TypeInformation.of(new TypeHint<String>() {
						}));
			} else {
				sumAndCountStateDescriptor =
					new ValueStateDescriptor<String>(
						"recordStateDescriptor",
						TypeInformation.of(new TypeHint<String>() {
						}));
			}

			recordState = getRuntimeContext().getState(sumAndCountStateDescriptor);
		}

		@Override
		public void flatMap(RescaleStructAuction input, Collector<String> collector) throws Exception {

			String records = recordState.value();

			if (null == records) {
				records = "";
			}

			records += ":" + (int) input.price;
			String[] strs = records.split(":");
			if (strs.length > 10) {
				records = records.substring(records.indexOf(":") + 1);
			}

			recordState.update(records);

//			collector.collect(String.valueOf(sumAndCount.f0 / sumAndCount.f1));
			collector.collect(input.timestamp + ":" + System.currentTimeMillis());
		}
	}
}
