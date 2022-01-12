package org.apache.flink.streaming.examples.rescale;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * NexmarkQ7.
 */
public class NexmarkQ7 {
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
		FlinkKafkaConsumer<String> bidConsumer = Utils.getConsumer(params, "bidTopic", "bidTopic");

		DataStream<RescaleStructBid> bidStream = env
			.addSource(bidConsumer)
			.flatMap(new Utils.KafkaBidParser(true))
			.rebalance();

//		bidStream
//			.keyBy(bid -> bid.itemId)
//			.window(TumblingProcessingTimeWindows.of(Time.milliseconds(slidingWindowStep)))
//			.max("price")
//			.name("window")
//			.setParallelism(windowPar)
//		;
		DataStream<RescaleStructBid> maxDataStream = bidStream
//			.keyBy(bid -> bid.itemId)
//			.windowAll(Utils.OutputTumblingWindow.of(Time.milliseconds(slidingWindowStep)))
			.windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(slidingWindowStep)))
			.max("price")
//			.apply(new Utils.BidMaxAllWindowFunction())
			.name("WindowAndMax");
		bidStream
			.join(maxDataStream)
			.where(bid -> bid.price)
			.equalTo(maxBid -> maxBid.price)
//			.window(Utils.OutputTumblingWindow.of(Time.milliseconds(slidingWindowStep)))
			.window(TumblingProcessingTimeWindows.of(Time.milliseconds(slidingWindowStep)))
			.with((bid1, bid2) -> {
				bid1.timestampInfo = bid1.timestampInfo + "-" + System.currentTimeMillis();
				return bid1;
			})
			.name("window")
//			.print()
		;

		env.execute("Nexmark q7");
	}
}
