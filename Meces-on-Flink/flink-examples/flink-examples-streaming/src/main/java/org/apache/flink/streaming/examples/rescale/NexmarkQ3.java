package org.apache.flink.streaming.examples.rescale;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * Nexmark q3.
 */
public class NexmarkQ3 {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		int windowPara = Integer.parseInt(params.get("opPar", "1"));
		int sinkInterval = Integer.parseInt(params.get("sinkInterval", "1000"));

		// env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// kafka consumer
		FlinkKafkaConsumer<String> auctionConsumer = Utils.getConsumer(params, "auctionTopic", "auctionTopic");
		FlinkKafkaConsumer<String> personConsumer = Utils.getConsumer(params, "personTopic", "personTopic");

		// not exactly like nexmark
		DataStream<RescaleStructAuction> auctionSourceStream = env
			.addSource(auctionConsumer)
			.flatMap(new Utils.KafkaAuctionParser())
			.filter(new FilterFunction<RescaleStructAuction>() {
				private static final long serialVersionUID = 1L;

				@Override
				public boolean filter(RescaleStructAuction value) {
					return value.timestamp % 100 == 0;
				}
			});
		DataStream<RescaleStructPerson> personSourceStream = env
			.addSource(personConsumer)
			.flatMap(new Utils.KafkaPersonParser())
//			.filter(new FilterFunction<RescaleStructPerson>() {
//				private static final long serialVersionUID = 1L;
//
//				@Override
//				public boolean filter(RescaleStructPerson value) {
//					return value.dateTime % 100 == 0;
//				}
//			})
			;

//		auctionSourceStream
//			.keyBy(auction -> auction.sellerId)
//			.intervalJoin(personSourceStream.keyBy(person -> person.id))
//			.between(Time.seconds(-30), Time.seconds(30))
//			.process(new ProcessJoinFunction<RescaleStructAuction, RescaleStructPerson, String>() {
//				@Override
//				public void processElement(RescaleStructAuction auction, RescaleStructPerson person, Context context, Collector<String> collector) throws Exception {
//					collector.collect(person.name + ":" + auction.timestamp + ":" + System.currentTimeMillis());
//				}
//			})
//			.print()
//		;

		auctionSourceStream
			.join(personSourceStream)
			.where(auction -> auction.sellerId)
			.equalTo(person -> person.id)
//			.window(TumblingProcessingTimeWindows.of(Time.milliseconds(10000)))
			.window(Utils.OutputTumblingWindow.of(Time.milliseconds(10000)))
			.with(new JoinFunction<RescaleStructAuction, RescaleStructPerson, Tuple4<Long, String, Long, Long>>() {
				@Override
				public Tuple4<Long, String, Long, Long> join(
					RescaleStructAuction a,
					RescaleStructPerson p) throws Exception {
					return new Tuple4<Long, String, Long, Long>(a.id, p.name, a.timestamp, System.currentTimeMillis());
				}
			})
			.name("window")
			.setParallelism(windowPara)
//			.print()
		;
		env.execute("Nexmark q3");
	}
}
