package org.apache.flink.streaming.examples.rescale;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * Nexmark q2.
 */
public class NexmarkQ2 {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		int filterPar = Integer.parseInt(params.get("opPar", "1"));
		int sinkInterval = Integer.parseInt(params.get("sinkInterval", "1000"));

		// env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// kafka consumer
		FlinkKafkaConsumer<String> consumer = Utils.getConsumer(params, "bidTopic", "bidTopic");

		env
			.addSource(consumer)
			.flatMap(new Utils.KafkaBidParser()).rebalance()
			.filter(new FilterFunction<RescaleStructBid>() {
				private static final long serialVersionUID = 1L;

				@Override
				public boolean filter(RescaleStructBid value) {
//					return value.itemId == 1007
//						|| value.itemId == 1020
//						|| value.itemId == 2001
//						|| value.itemId == 2019
//						|| value.itemId == 1087;
					// no need to strictly follow the filter function in Nexmark paper
					return value.itemId > 0;
				}
			}).setParallelism(filterPar).name("Filter")
			.rebalance()
			.addSink(new Utils.SelectivePrintSink(sinkInterval)).setParallelism(filterPar).name("Sink")
		;

		env.execute("Nexmark q2");
	}
}
