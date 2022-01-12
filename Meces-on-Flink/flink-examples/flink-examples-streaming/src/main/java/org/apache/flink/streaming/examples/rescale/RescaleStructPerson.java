package org.apache.flink.streaming.examples.rescale;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RescaleStructPerson {
	public long id;
	public String name;
	public long dateTime;

	// public constructor to make it a Flink POJO
	public RescaleStructPerson() {}

	public RescaleStructPerson(long id, String name, long dateTime) {
		this.id = id;
		this.name = name;
		this.dateTime = dateTime;
	}

	@Override
	public String toString() {
		return "id: " + id +
			"name: " + name +
			"dateTime: " + dateTime;
	}

	static class RescaleStructPersonSource implements SourceFunction<RescaleStructPerson> {

		private volatile boolean isRunning = true;
		private Random rnd = new Random();
		private final int lineIntervalMilliSeconds;
		private final String[] names = new String[]{"Peter", "Paul", "Luke", "John", "Saul", "Vicky", "Kate", "Julie", "Sarah", "Deiter", "Walter"};

		RescaleStructPersonSource(int lineIntervalMilliSeconds) {
			this.lineIntervalMilliSeconds = lineIntervalMilliSeconds;
		}

		@Override
		public void run(SourceContext<RescaleStructPerson> ctx) throws Exception {
			while (isRunning) {
				long id = rnd.nextLong();
				String name = names[rnd.nextInt(names.length)];
				long dateTime = System.currentTimeMillis();

				ctx.collect(new RescaleStructPerson(id, name, dateTime));
				Thread.sleep(lineIntervalMilliSeconds);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

}
