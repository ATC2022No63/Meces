package org.apache.flink.runtime.state.redis;

public class Utils {
	public Utils() {
	}

	public static void batchInput(BasicKVDatabaseClient dbClient, byte[][] keys, byte[][] values, int batchSize) throws Exception {
		int to;
		for(int i = 0; i < keys.length; i = to) {
			to = i + batchSize < keys.length ? i + batchSize : keys.length;
			dbClient.putAll(keys, values, i, to);
		}

	}

	public static void batchRemove(BasicKVDatabaseClient dbClient, byte[][] keys, int batchSize) throws Exception {
		int to;
		for(int i = 0; i < keys.length; i = to) {
			to = i + batchSize < keys.length ? i + batchSize : keys.length;
			dbClient.delAll(keys, i, to);
		}

	}
}
