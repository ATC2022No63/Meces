package org.apache.flink.runtime.state.redis;

public abstract class BasicKVDatabaseClient {
	public BasicKVDatabaseClient() {
	}

	public abstract byte[] get(byte[] var1) throws Exception;

	public abstract void put(byte[] var1, byte[] var2) throws Exception;

	public abstract boolean del(byte[] var1) throws Exception;

	public byte[][] getAll(byte[][] keys) throws Exception {
		byte[][] result = new byte[keys.length][];

		for (int i = 0; i < keys.length; ++i) {
			result[i] = this.get(keys[i]);
		}

		return result;
	}

	public byte[][] getAll(byte[][] keys, int from, int to) throws Exception {
		assert from >= 0 && from < to;

		assert keys.length <= to;

		byte[][] queryKeys = new byte[to - from][];

		for (int i = from; i < to; ++i) {
			queryKeys[i - from] = keys[i];
		}

		byte[][] result = this.getAll(queryKeys);
		return result;
	}

	public void putAll(byte[][] keys, byte[][] values) throws Exception {
		assert keys.length == values.length;

		for (int i = 0; i < keys.length; ++i) {
			this.put(keys[i], values[i]);
		}

	}

	public void putAll(byte[][] keys, byte[][] values, int from, int to) throws Exception {
		assert keys.length == values.length;

		assert from >= 0 && from < to;

		assert to <= keys.length;

		byte[][] insertKeys = new byte[to - from][];
		byte[][] insertValues = new byte[to - from][];

		for (int i = from; i < to; ++i) {
			insertKeys[i - from] = keys[i];
			insertValues[i - from] = values[i];
		}

		this.putAll(insertKeys, insertValues);
	}

	public void delAll(byte[][] keys) throws Exception {
		for (int i = 0; i < keys.length; ++i) {
			this.del(keys[i]);
		}

	}

	public void delAll(byte[][] keys, int from, int to) throws Exception {
		assert from >= 0 && from < to;

		assert to <= keys.length;

		byte[][] removeKeys = new byte[to - from][];

		for (int i = from; i < to; ++i) {
			removeKeys[i - from] = keys[i];
		}

		this.delAll(removeKeys);
	}

	public abstract void close() throws Exception;

	public abstract void connect() throws Exception;

	public abstract void clearDB() throws Exception;
}
