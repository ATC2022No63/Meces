package org.apache.flink.streaming.examples.rescale;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Utils.
 */
public class Utils {
	/**
	 * KafkaBidParser.
	 */
	public static class KafkaBidParser extends RichFlatMapFunction<String, RescaleStructBid> {

		Random rnd = new Random();
		final boolean output;

		public KafkaBidParser() {
			this.output = false;
		}

		public KafkaBidParser(boolean output) {
			this.output = output;
		}

		@Override
		public void flatMap(String input, Collector<RescaleStructBid> out) {
			String[] content = input.split(":");
			if (output) {
				if (rnd.nextInt(100000) == 1) {
					System.out.println(
						content[4] + ":" + System.currentTimeMillis());
				}
			}
			out.collect(new RescaleStructBid(
				Long.parseLong(content[0]),
				Long.parseLong(content[1]),
				Integer.parseInt(content[2]),
				Long.parseLong(content[3]),
				content[4]));
		}
	}

	/**
	 * SelectivePrintSink.
	 */
	public static class SelectivePrintSink extends RichSinkFunction<RescaleStructBid> {
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
		public void invoke(RescaleStructBid value, Context context) {
			if (shouldOutput) {
				System.out.println(value.timestampInfo + ":" + System.currentTimeMillis());
				shouldOutput = false;
			}
		}

		@Override
		public void close() throws Exception {
			super.close();
		}
	}

	public static StreamExecutionEnvironment getEnv() {
		return StreamExecutionEnvironment.getExecutionEnvironment();
	}

	/**
	 * FlinkKafkaConsumer.
	 */
	public static FlinkKafkaConsumer<String> getConsumer(ParameterTool params, String topicParamKey, String defaultTopicName) {
		Properties props = new Properties();
		String kafkaServers = params.get("kafkaServers", "localhost:9092");
		props.setProperty("bootstrap.servers", kafkaServers);
		props.setProperty("group.id", "flink-group");
		FlinkKafkaConsumer<String> consumer =
			new FlinkKafkaConsumer<>(params.get(topicParamKey, defaultTopicName), new SimpleStringSchema(), props);
		consumer.setStartFromLatest();
		return consumer;
	}

	/**
	 * KafkaPersonParser.
	 */
	public static class KafkaPersonParser extends RichFlatMapFunction<String, RescaleStructPerson> {

		@Override
		public void flatMap(String input, Collector<RescaleStructPerson> out) {
			String[] content = input.split(":");
			out.collect(new RescaleStructPerson(
				Long.parseLong(content[0]),
				content[1],
				Long.parseLong(content[2])));
		}
	}

	/**
	 * KafkaAuctionParser.
	 */
	public static class KafkaAuctionParser extends RichFlatMapFunction<String, RescaleStructAuction> {

		@Override
		public void flatMap(String input, Collector<RescaleStructAuction> out) {
			String[] content = input.split(":");
			out.collect(new RescaleStructAuction(
				Long.parseLong(content[0]),
				Long.parseLong(content[1]),
				Long.parseLong(content[2]),
				Long.parseLong(content[3]),
				Long.parseLong(content[4])));
		}
	}

	/**
	 * OutputTumblingWindow.
	 */
	public static class OutputTumblingWindow extends WindowAssigner<Object, TimeWindow> {
		Random rnd = new Random();
		private static final long serialVersionUID = 1L;

		private final long size;

		private final long globalOffset;

		private Long staggerOffset = null;

		private final WindowStagger windowStagger;

		private OutputTumblingWindow(long size, long offset, WindowStagger windowStagger) {
			if (Math.abs(offset) >= size) {
				throw new IllegalArgumentException("TumblingProcessingTimeWindows parameters must satisfy abs(offset) < size");
			}

			this.size = size;
			this.globalOffset = offset;
			this.windowStagger = windowStagger;
		}

		@Override
		public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
			if (element instanceof RescaleStructAuction) {
				System.out.println(((RescaleStructAuction) element).timestamp + ":" + System.currentTimeMillis());
			} else if (element instanceof CoGroupedStreams.TaggedUnion) {
				Object oneElement = ((CoGroupedStreams.TaggedUnion) element).getOne();
				if (oneElement instanceof RescaleStructAuction) {
					RescaleStructAuction auction = (RescaleStructAuction) oneElement;
					System.out.println(auction.timestamp + ":" + System.currentTimeMillis());
				} else if (oneElement instanceof RescaleStructBid) {
					RescaleStructBid bid = (RescaleStructBid) oneElement;
					if (rnd.nextInt(100000) == 1) {
						System.out.println(
							bid.timestampInfo + ":" + System.currentTimeMillis());
					}
				}
			} else if (element instanceof RescaleStructBid) {
				System.out.println(((RescaleStructBid) element).timestampInfo + ":" + System.currentTimeMillis());
			}
			final long now = context.getCurrentProcessingTime();
			if (staggerOffset == null) {
				staggerOffset = windowStagger.getStaggerOffset(context.getCurrentProcessingTime(), size);
			}
			long start = TimeWindow.getWindowStartWithOffset(now, (globalOffset + staggerOffset) % size, size);
			return Collections.singletonList(new TimeWindow(start, start + size));
		}

		public long getSize() {
			return size;
		}

		@Override
		public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
			return ProcessingTimeTrigger.create();
		}

		@Override
		public String toString() {
			return "TumblingProcessingTimeWindows(" + size + ")";
		}

		public static OutputTumblingWindow of(Time size) {
			return new OutputTumblingWindow(size.toMilliseconds(), 0, WindowStagger.ALIGNED);
		}

		public static OutputTumblingWindow of(Time size, Time offset) {
			return new OutputTumblingWindow(size.toMilliseconds(), offset.toMilliseconds(), WindowStagger.ALIGNED);
		}

		@PublicEvolving
		public static OutputTumblingWindow of(Time size, Time offset, WindowStagger windowStagger) {
			return new OutputTumblingWindow(size.toMilliseconds(), offset.toMilliseconds(), windowStagger);
		}

		@Override
		public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
			return new TimeWindow.Serializer();
		}

		@Override
		public boolean isEventTime() {
			return false;
		}
	}

	/**
	 * OutputSlidingProcessingTimeWindows.
	 */
	public static class OutputSlidingProcessingTimeWindows extends WindowAssigner<Object, TimeWindow> {
		private static final long serialVersionUID = 1L;

		private final long size;

		private final long offset;

		private final long slide;

		private OutputSlidingProcessingTimeWindows(long size, long slide, long offset) {
			if (Math.abs(offset) >= slide || size <= 0) {
				throw new IllegalArgumentException("SlidingProcessingTimeWindows parameters must satisfy " +
					"abs(offset) < slide and size > 0");
			}

			this.size = size;
			this.slide = slide;
			this.offset = offset;
		}

		@Override
		public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
			if (element instanceof RescaleStructAuction) {
				System.out.println(((RescaleStructAuction) element).timestamp + ":" + System.currentTimeMillis());
			} else if (element instanceof CoGroupedStreams.TaggedUnion) {
				RescaleStructAuction auction = (RescaleStructAuction) ((CoGroupedStreams.TaggedUnion) element).getOne();
				System.out.println(auction.timestamp + ":" + System.currentTimeMillis());
			}
			timestamp = context.getCurrentProcessingTime();
			List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
			long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
			for (long start = lastStart; start > timestamp - size; start -= slide) {
				windows.add(new TimeWindow(start, start + size));
			}
			return windows;
		}

		public long getSize() {
			return size;
		}

		public long getSlide() {
			return slide;
		}

		@Override
		public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
			return ProcessingTimeTrigger.create();
		}

		@Override
		public String toString() {
			return "SlidingProcessingTimeWindows(" + size + ", " + slide + ")";
		}

		public static OutputSlidingProcessingTimeWindows of(Time size, Time slide) {
			return new OutputSlidingProcessingTimeWindows(size.toMilliseconds(), slide.toMilliseconds(), 0);
		}

		public static OutputSlidingProcessingTimeWindows of(Time size, Time slide, Time offset) {
			return new OutputSlidingProcessingTimeWindows(size.toMilliseconds(), slide.toMilliseconds(), offset.toMilliseconds());
		}

		@Override
		public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
			return new TimeWindow.Serializer();
		}

		@Override
		public boolean isEventTime() {
			return false;
		}
	}

	/**
	 * SelectiveStringPrintSink.
	 */
	public static class SelectiveStringPrintSink extends RichSinkFunction<String> {
		private boolean shouldOutput = false;
		private final long outputInterval;

		SelectiveStringPrintSink(long outputInterval) {
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
		public void invoke(String value, Context context) {
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

	static class BidMaxFunction extends ProcessAllWindowFunction<RescaleStructBid, RescaleStructBid, TimeWindow> {

		public BidMaxFunction() {
		}

		@Override
		public void process(
			ProcessAllWindowFunction<RescaleStructBid, RescaleStructBid, TimeWindow>.Context arg0,
			Iterable<RescaleStructBid> input,
			Collector<RescaleStructBid> out) {
			out.collect(getMaxBid(input));
		}

	}

	static class BidMaxAllWindowFunction implements AllWindowFunction<RescaleStructBid, RescaleStructBid, TimeWindow> {
		@Override
		public void apply(
			TimeWindow window,
			Iterable<RescaleStructBid> values,
			Collector<RescaleStructBid> out) {
			out.collect(Utils.getMaxBid(values));
		}
	}

	private static RescaleStructBid getMaxBid(Iterable<RescaleStructBid> input) {
		RescaleStructBid maxBid = null;
		int maxCount = 0;
		for (RescaleStructBid b : input) {
			if (b.itemNum > maxCount) {
				maxCount = b.itemNum;
				maxBid = b;
			}
		}
		return maxBid;
	}
}
