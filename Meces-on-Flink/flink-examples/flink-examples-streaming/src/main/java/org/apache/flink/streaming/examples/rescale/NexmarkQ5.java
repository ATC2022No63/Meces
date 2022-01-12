package org.apache.flink.streaming.examples.rescale;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class NexmarkQ5 {
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		final int slidingWindowSize = Integer.parseInt(params.get("windowSize", "1000"));
		final int slidingWindowStep = Integer.parseInt(params.get("windowStep", "100"));
		final int lineIntervalMilliSeconds = Integer.parseInt(params.get("interval", "500"));

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.disableOperatorChaining();
		DataStream<RescaleStructBid> sourceStream = env
			.addSource(new RescaleStructBid.RescaleStructBidSource(lineIntervalMilliSeconds))
			.name("Source")
			.rebalance();

		DataStream<RescaleStructBid> countDataStream = sourceStream
			.keyBy(value -> value.itemId)
			.window(SlidingProcessingTimeWindows.of(Time.milliseconds(slidingWindowSize), Time.milliseconds(slidingWindowStep)))
			.sum("itemNum")
			.name("WindowAndSum1");

		DataStream<RescaleStructBid> maxDataStream = countDataStream
			.windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(slidingWindowStep)))
			.process(new Utils.BidMaxFunction())
			.name("WindowAndMax");
//		DataStream<RescaleStructBid> maxDataStream =  sourceStream
//			.keyBy(value -> value.itemId)
//			.window(SlidingProcessingTimeWindows.of(Time.milliseconds(slidingWindowSize), Time.milliseconds(slidingWindowStep)))
//			.sum("itemNum")
//			.name("WindowAndSum2")
//			.windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(slidingWindowStep)))
//			.process(new MaxFunction())
//			.name("WindowAndMax");

		maxDataStream
			.join(countDataStream)
			.where(bid -> bid.itemNum)
			.equalTo(bid -> bid.itemNum)
			.window(TumblingProcessingTimeWindows.of(Time.milliseconds(slidingWindowStep)))
			.with((bid1, bid2) -> {
				bid1.timestampInfo = bid1.timestampInfo + "-" + System.currentTimeMillis();
				return bid1;
			})
			.name("window")
			.print();

//		dataStream.addSink(new DiscardingSink<>());

		env.execute("Nexmark q5");
	}

}
