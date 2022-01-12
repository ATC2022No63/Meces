package org.apache.flink.streaming.examples.rescale;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RescaleStructBid {
	public long itemId;
	public long bidderId;
	public int price;
	public long  bidTime;
	public int itemNum = 1;
	public String timestampInfo = "";

	// public constructor to make it a Flink POJO
	public RescaleStructBid() {}

	public RescaleStructBid(long itemId, long bidderId, int price, long bidTime, String timestampInfo) {
		this.itemId = itemId;
		this.bidderId = bidderId;
		this.price = price;
		this.bidTime = bidTime;
		this.timestampInfo = timestampInfo;
	}

	@Override
	public String toString() {
		return "itemId: " + itemId +
			"bidderId: " + bidderId +
			"price: " + price +
			"bidTime: " + bidTime +
			"itemNum: " + itemNum +
			"timestampInfo: " + timestampInfo;
	}

	static class RescaleStructBidSource implements SourceFunction<RescaleStructBid> {

		private volatile boolean isRunning = true;
		private Random rnd = new Random();
		private final int lineIntervalMilliSeconds;
		private final long[] itemIds = new long[]{1, 2, 3};

		RescaleStructBidSource(int lineIntervalMilliSeconds) {
			this.lineIntervalMilliSeconds = lineIntervalMilliSeconds;
		}

		@Override
		public void run(SourceContext<RescaleStructBid> ctx) throws Exception {
			while (isRunning) {
				long itemId = itemIds[rnd.nextInt(itemIds.length)];
				long bidderId = rnd.nextLong();
				int price = rnd.nextInt();
				long bidTime = rnd.nextLong();

				ctx.collect(new RescaleStructBid(itemId, bidderId, price, bidTime, "" + System.currentTimeMillis()));
				Thread.sleep(lineIntervalMilliSeconds);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}
