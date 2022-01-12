package org.apache.flink.runtime.state.redis;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

public class TestRedisCluster {
	public TestRedisCluster() {
	}

	public static int byteArrayToInt(byte[] b) {
		return b[3] & 255 | (b[2] & 255) << 8 | (b[1] & 255) << 16 | (b[0] & 255) << 24;
	}

	public static byte[] intToByteArray(int a) {
		return new byte[]{(byte) (a >> 24 & 255), (byte) (a >> 16 & 255), (byte) (a >> 8 & 255), (byte) (a & 255)};
	}

	public static void main(String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String str = br.readLine();
		System.out.println("Hello " + str + ".");

		Logger logger = Logger.getLogger("Test Redis Cluster");
		logger.info("Start preparing data...");
		Set<Integer> keySet = new HashSet();
		ArrayList<ArrayList<Tuple<Integer, byte[]>>> data = new ArrayList();
		Random random = new Random(System.nanoTime());

		int keyInt;
		for (int i = 0; i < 100; ++i) {
			int randomItemNum = Math.abs(random.nextInt() % 500) + 1;
			ArrayList<TestRedisCluster.Tuple<Integer, byte[]>> items = new ArrayList(randomItemNum);

			for (int j = 0; j < randomItemNum; ++j) {
				for (keyInt = Math.abs(random.nextInt()); keySet.contains(keyInt); keyInt = Math.abs(random.nextInt())) {
				}

				keySet.add(keyInt);
				byte[] key = intToByteArray(keyInt);
				random.nextBytes(key);
				int randomValueLength = Math.abs(random.nextInt() % 50) + 1;
				byte[] value = new byte[randomValueLength];
				random.nextBytes(value);
				items.add(new TestRedisCluster.Tuple(keyInt, value));
			}

			data.add(items);
		}

		logger.info("Start clearning database...");
		ShardedRedisClusterClient.getProcessLevelClient().clearDB();
		logger.info("Start clearning database...");
		ShardedRedisClusterClient.getProcessLevelClient().clearDB();
		logger.info("Start batch inserting...");
		data.parallelStream().forEach((itemsx) -> {
			try {
				BasicKVDatabaseClient client = ShardedRedisClusterClient.getProcessLevelClient();
				byte[][] keys = new byte[itemsx.size()][];
				byte[][] values = new byte[itemsx.size()][];

				for (int i = 0; i < itemsx.size(); ++i) {
					keys[i] = intToByteArray((Integer) ((TestRedisCluster.Tuple) itemsx.get(i)).x);
					values[i] = (byte[]) ((TestRedisCluster.Tuple) itemsx.get(i)).y;
				}

				Utils.batchInput(client, keys, values, 20);
			} catch (Exception var5) {
				var5.printStackTrace();
			}

		});
		logger.info("Start batch fetching...");
		data.parallelStream().forEach((itemsx) -> {
			try {
				BasicKVDatabaseClient client = ShardedRedisClusterClient.getProcessLevelClient();
				byte[][] keys = new byte[itemsx.size()][];

				int i;
				for (i = 0; i < itemsx.size(); ++i) {
					keys[i] = intToByteArray((Integer) ((TestRedisCluster.Tuple) itemsx.get(i)).x);
				}

				byte[][] values = client.getAll(keys);

				for (i = 0; i < itemsx.size(); ++i) {
					assert Arrays.equals(values[i], (byte[]) ((TestRedisCluster.Tuple) itemsx.get(i)).y);
				}
			} catch (Exception var5) {
				var5.printStackTrace();
			}

		});
		logger.info("Start testing null key fetch");
		int nullKey;
		byte[][] nullKeys = new byte[][]{intToByteArray(-1), intToByteArray(-2), intToByteArray(-3)};
		byte[][] results = ShardedRedisClusterClient.getProcessLevelClient().getAll(nullKeys);
		byte[][] var16 = results;
		keyInt = results.length;

		for (int var17 = 0; var17 < keyInt; ++var17) {
			byte[] r = var16[var17];
			logger.info("Null fetch result:" + r);
		}

		logger.info("Done!");
	}

	static class Tuple<X, Y> {
		public final X x;
		public final Y y;

		public Tuple(X x, Y y) {
			this.x = x;
			this.y = y;
		}
	}
}
