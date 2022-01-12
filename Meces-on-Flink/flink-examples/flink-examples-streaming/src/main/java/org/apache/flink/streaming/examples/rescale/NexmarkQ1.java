package org.apache.flink.streaming.examples.rescale;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Nexmark q1.
 */
public class NexmarkQ1 {
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		int lineIntervalMilliSeconds = Integer.parseInt(params.get("interval", "500"));
		int counterPar = Integer.parseInt(params.get("counterPar", "1"));
		int defaultPar = Integer.parseInt(params.get("defaultPar", "1"));
		int sinkInterval = Integer.parseInt(params.get("sinkInterval", "1000"));

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		String kafkaServers = params.get("kafkaServers", "localhost:9092");
		props.setProperty("bootstrap.servers", kafkaServers);
		props.setProperty("group.id", "flink-group");
		FlinkKafkaConsumer<String> consumer =
			new FlinkKafkaConsumer<>(params.get("bidTopic", "bidTopic"), new SimpleStringSchema(), props);
		consumer.setStartFromLatest();

		env
//			.addSource(new RescaleStructBid.RescaleStructBidSource(lineIntervalMilliSeconds))
			.addSource(consumer)
			.flatMap(new Utils.KafkaBidParser()).rebalance()
			.flatMap(new CurrencyConversionFlatMap()).setParallelism(counterPar).name("FlatMap-CurrencyConversion")
			.addSink(new Utils.SelectivePrintSink(sinkInterval)).setParallelism(counterPar).name("Sink")
			;

		env.execute("Nexmark q1");
	}

	static class CurrencyConversionFlatMap extends RichFlatMapFunction<RescaleStructBid, RescaleStructBid> {

		@Override
		public void flatMap(RescaleStructBid input, Collector<RescaleStructBid> collector) {
			input.price *= 0.908;
			collector.collect(input);
		}
	}

}
