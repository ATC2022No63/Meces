package org.apache.flink.streaming.examples.rescale;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class NexmarkQ8 {
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final int slidingWindowSize = Integer.parseInt(params.get("windowSize", "5"));
		final int slidingWindowStep = Integer.parseInt(params.get("windowStep", "100"));
		final int windowPara = Integer.parseInt(params.get("windowPara", "1"));

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.disableOperatorChaining();

		String kafkaServers = params.get("kafkaServers", "localhost:9092");
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", kafkaServers);
		props.setProperty("group.id", "flink-group");
		FlinkKafkaConsumer<String> auctionConsumer =
			new FlinkKafkaConsumer<>(params.get("auctionTopic", "auctionTopic"), new SimpleStringSchema(), props);
		FlinkKafkaConsumer<String> personConsumer =
			new FlinkKafkaConsumer<>(params.get("personTopic", "personTopic"), new SimpleStringSchema(), props);
		auctionConsumer.setStartFromLatest();
		personConsumer.setStartFromLatest();

		DataStream<RescaleStructAuction> auctionSourceStream = env
			.addSource(auctionConsumer)
			.flatMap(new Utils.KafkaAuctionParser())
			.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<RescaleStructAuction>() {
				Long currentMaxTimestamp = 0L;
				final Long maxOutOfOrder = 5000L;
				@Override
				public Watermark getCurrentWatermark() {
					return new Watermark(currentMaxTimestamp - maxOutOfOrder);
				}

				@Override
				public long extractTimestamp(RescaleStructAuction element, long recordTimestamp) {
					long timestamp = element.timestamp;
					currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
					return timestamp;
				}
			})
			.name("AuctionStream");
		DataStream<RescaleStructPerson> personSourceStream = env
			.addSource(personConsumer)
			.flatMap(new Utils.KafkaPersonParser())
			.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<RescaleStructPerson>() {
				Long currentMaxTimestamp = 0L;
				final Long maxOutOfOrder = 5000L;
				@Override
				public Watermark getCurrentWatermark() {
					return new Watermark(currentMaxTimestamp - maxOutOfOrder);
				}

				@Override
				public long extractTimestamp(RescaleStructPerson element, long recordTimestamp) {
					long timestamp = element.dateTime;
					currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
					return timestamp;
				}
			})
			.name("PersonStream");

		auctionSourceStream
			.join(personSourceStream)
			.where(auction -> auction.sellerId)
			.equalTo(person -> person.id)
			.window(SlidingProcessingTimeWindows.of(Time.seconds(slidingWindowSize), Time.milliseconds(slidingWindowStep)))
//			.window(TumblingEventTimeWindows.of(Time.seconds(10)))
//			.allowedLateness()
//			.evictor(TimeEvictor.of(Time.seconds(1)))
//			.evictor(new Evictor<CoGroupedStreams.TaggedUnion<RescaleStructAuction, RescaleStructPerson>, TimeWindow>() {
//				@Override
//				public void evictBefore(
//					Iterable<TimestampedValue<CoGroupedStreams.TaggedUnion<RescaleStructAuction, RescaleStructPerson>>> elements,
//					int size,
//					TimeWindow window,
//					EvictorContext evictorContext) {
//
////					System.out.println("evict-" + System.currentTimeMillis() + ":" + window.maxTimestamp());
//					long maxTime = Long.MIN_VALUE;
//					for (Iterator<TimestampedValue<CoGroupedStreams.TaggedUnion<RescaleStructAuction, RescaleStructPerson>>> iterator = elements.iterator(); iterator.hasNext();){
//						RescaleStructAuction auction = iterator.next().getValue().getOne();
//						if (auction != null) {
//							maxTime = Math.max(maxTime, auction.timestamp);
//						}
//					}
//
//					for (Iterator<TimestampedValue<CoGroupedStreams.TaggedUnion<RescaleStructAuction, RescaleStructPerson>>> iterator = elements.iterator(); iterator.hasNext(); ) {
//						TimestampedValue<CoGroupedStreams.TaggedUnion<RescaleStructAuction, RescaleStructPerson>> record = iterator.next();
//						if (record.getValue().isOne() && record.getValue().getOne().timestamp < maxTime) {
//							iterator.remove();
//						}
//					}
//				}
//
//				@Override
//				public void evictAfter(
//					Iterable<TimestampedValue<CoGroupedStreams.TaggedUnion<RescaleStructAuction, RescaleStructPerson>>> elements,
//					int size,
//					TimeWindow window,
//					EvictorContext evictorContext) {
//				}
//			})
			.with(new JoinFunction<RescaleStructAuction, RescaleStructPerson, Tuple4<Long, String, Long, Long>>() {
				@Override
				public Tuple4<Long, String, Long, Long> join(
					RescaleStructAuction a,
					RescaleStructPerson p) throws Exception {
					return new Tuple4<Long, String, Long, Long>(a.id, p.name, a.timestamp, System.currentTimeMillis());
				}
			})
//			.windowAll(SlidingProcessingTimeWindows.of(Time.milliseconds(100), Time.milliseconds(100)))
//			.process(new MaxFunction())
//			.setParallelism(env.getParallelism())
//			.apply((auction, p) -> {
//				return new Tuple2<Long, String>(p.id, p.name);
//			})
			.setParallelism(windowPara)
			.name("window")
//			.print()
		;

//		dataStream.addSink(new DiscardingSink<>());

		env.execute("Nexmark q8");
	}

	private static class MaxFunction extends ProcessAllWindowFunction<Tuple4<Long, String, Long, Long>, Tuple4<Long, String, Long, Long>, TimeWindow> {

		public MaxFunction() {
		}

		@Override
		public void process(
			ProcessAllWindowFunction<Tuple4<Long, String, Long, Long>, Tuple4<Long, String, Long, Long>, TimeWindow>.Context arg0,
			Iterable<Tuple4<Long, String, Long, Long>> input,
			Collector<Tuple4<Long, String, Long, Long>> out) {

			Tuple4<Long, String, Long, Long> maxBid = null;
			long maxTime = 0;
			for (Tuple4<Long, String, Long, Long> b : input) {
				if (b.f2 > maxTime) {
					maxTime = b.f2;
					maxBid = b;
				}
			}
			out.collect(maxBid);
		}

	}
}
