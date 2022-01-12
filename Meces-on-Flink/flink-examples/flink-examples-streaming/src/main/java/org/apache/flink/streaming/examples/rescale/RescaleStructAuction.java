package org.apache.flink.streaming.examples.rescale;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * RescaleStructAuction.
 */
public class RescaleStructAuction {
	long id;
	long sellerId;
	long timestamp;
	long category;
	long price;

	// public constructor to make it a Flink POJO
	public RescaleStructAuction() {}

	public RescaleStructAuction(long id, long sellerId, long category, long price,  long timestamp) {
		this.id = id;
		this.sellerId = sellerId;
		this.category = category;
		this.price = price;
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "id: " + id +
			"sellerId: " + sellerId +
			"timestamp: " + timestamp;
	}

	static class RescaleStructAuctionSource implements SourceFunction<RescaleStructAuction> {

		private volatile boolean isRunning = true;
		private Random rnd = new Random();
		private final int lineIntervalMilliSeconds;

		RescaleStructAuctionSource(int lineIntervalMilliSeconds) {
			this.lineIntervalMilliSeconds = lineIntervalMilliSeconds;
		}

		@Override
		public void run(SourceContext<RescaleStructAuction> ctx) throws Exception {
			while (isRunning) {
				long id = rnd.nextLong();
				long sellerId = rnd.nextLong();
				long category = rnd.nextLong();
				long price = rnd.nextLong();

				ctx.collect(new RescaleStructAuction(id, sellerId, category, price, System.currentTimeMillis()));
				Thread.sleep(lineIntervalMilliSeconds);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}
