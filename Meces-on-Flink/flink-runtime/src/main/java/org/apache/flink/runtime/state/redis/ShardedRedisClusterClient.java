package org.apache.flink.runtime.state.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.ShardedJedisPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ShardedRedisClusterClient extends BasicKVDatabaseClient{
	public static final String CONF_POOL_SIZE = "redis.connection.pool.size";
	public static final String CONF_DB_ID = "redis.db.index";
	public static final String CONF_HOSTS_LIST = "redis.hosts.list";
	public static final int DEFAULT_TIME_OUT_IN_SEC = 36000;
	public static final int DEFAULT_REDIS_PORT = 6379;
	private List<String> hosts = new ArrayList();
	private int databaseID;
	private int poolSize = 4;
	private ShardedJedisPool pool;
	private Logger logger = Logger.getLogger(ShardedRedisClusterClient.class.getName());
	private static ShardedRedisClusterClient processLevelSingletonClient;

	public static ShardedRedisClusterClient getProcessLevelClient() throws Exception {
		if (processLevelSingletonClient != null) {
			return processLevelSingletonClient;
		} else {
			initProcessLevelClient();
			return processLevelSingletonClient;
		}
	}

	private static synchronized void initProcessLevelClient() throws Exception {
		if (processLevelSingletonClient == null) {
			Properties mecesProperties = ProcessLevelConf.getMecesConf();
			String hostsString = mecesProperties.getProperty(ProcessLevelConf.REDIS_HOSTS_KEY, "localhost");
			int dbID = Integer.parseInt(mecesProperties.getProperty(ProcessLevelConf.REDIS_DB_INDEX_KEY, "0"));
			int poolSize = Integer.parseInt(mecesProperties.getProperty(ProcessLevelConf.REDIS_CONNECTION_POOL_SIZE_KEY, "4"));
			List<String> hosts = Arrays.asList(hostsString.split(","));
			processLevelSingletonClient = new ShardedRedisClusterClient(hosts, dbID, poolSize);
			processLevelSingletonClient.connect();
		}

	}

	public ShardedRedisClusterClient(List<String> redisHosts, int databaseID, int poolSize) {
		this.hosts.addAll(redisHosts);
		this.databaseID = databaseID;
		this.poolSize = poolSize;
	}

	public byte[] get(byte[] key) throws Exception {
		byte[] value = null;
		ShardedJedis jedis = this.pool.getResource();
		Throwable var4 = null;

		try {
			value = jedis.get(key);
		} catch (Throwable var13) {
			var4 = var13;
			throw var13;
		} finally {
			if (jedis != null) {
				if (var4 != null) {
					try {
						jedis.close();
					} catch (Throwable var12) {
						var4.addSuppressed(var12);
					}
				} else {
					jedis.close();
				}
			}

		}

		return value;
	}

	public byte[][] getAll(byte[][] keys) throws Exception {
		byte[][] results = new byte[keys.length][];
		ShardedJedis jedis = this.pool.getResource();
		Throwable var4 = null;

		try {
			ShardedJedisPipeline pipeline = jedis.pipelined();
			Response<byte[]>[] responses = new Response[keys.length];

			int i;
			for (i = 0; i < keys.length; ++i) {
				responses[i] = pipeline.get(keys[i]);
			}

			pipeline.sync();

			for (i = 0; i < keys.length; ++i) {
				results[i] = (byte[]) responses[i].get();
			}

			return results;
		} catch (Throwable var15) {
			var4 = var15;
			throw var15;
		} finally {
			if (jedis != null) {
				if (var4 != null) {
					try {
						jedis.close();
					} catch (Throwable var14) {
						var4.addSuppressed(var14);
					}
				} else {
					jedis.close();
				}
			}

		}
	}

	public void put(byte[] key, byte[] value) throws Exception {
		ShardedJedis jedis = this.pool.getResource();
		Throwable var4 = null;
		try {
			jedis.set(key, value);
		} catch (Throwable var13) {
			var4 = var13;
			throw var13;
		} finally {
			if (jedis != null) {
				if (var4 != null) {
					try {
						jedis.close();
					} catch (Throwable var12) {
						var4.addSuppressed(var12);
					}
				} else {
					jedis.close();
				}
			}

		}

	}

	public void putAll(byte[][] keys, byte[][] values) throws Exception {
		assert keys.length == values.length;

		ShardedJedis jedis = this.pool.getResource();
		Throwable var4 = null;

		try {
			ShardedJedisPipeline pipeline = jedis.pipelined();

			for (int i = 0; i < keys.length; ++i) {
				pipeline.set(keys[i], values[i]);
			}

			pipeline.sync();
		} catch (Throwable var14) {
			var4 = var14;
			throw var14;
		} finally {
			if (jedis != null) {
				if (var4 != null) {
					try {
						jedis.close();
					} catch (Throwable var13) {
						var4.addSuppressed(var13);
					}
				} else {
					jedis.close();
				}
			}

		}
	}

	public boolean del(byte[] key) throws Exception {
		try {
			ShardedJedis jedis = this.pool.getResource();
			Throwable var3 = null;

			boolean var4;
			try {
				jedis.del(key);
				var4 = true;
			} catch (Throwable var14) {
				var3 = var14;
				throw var14;
			} finally {
				if (jedis != null) {
					if (var3 != null) {
						try {
							jedis.close();
						} catch (Throwable var13) {
							var3.addSuppressed(var13);
						}
					} else {
						jedis.close();
					}
				}

			}

			return var4;
		} catch (Exception var16) {
			System.out.println(var16);
			return false;
		}
	}

	public boolean delAll(byte[][] keys, byte[][] values) throws Exception {
		assert keys.length == values.length;

		try {
			ShardedJedis jedis = this.pool.getResource();
			Throwable var4 = null;

			try {
				ShardedJedisPipeline pipeline = jedis.pipelined();

				for (int i = 0; i < keys.length; ++i) {
					pipeline.del(keys[i]);
				}

				pipeline.sync();
				boolean var19 = true;
				return var19;
			} catch (Throwable var16) {
				var4 = var16;
				throw var16;
			} finally {
				if (jedis != null) {
					if (var4 != null) {
						try {
							jedis.close();
						} catch (Throwable var15) {
							var4.addSuppressed(var15);
						}
					} else {
						jedis.close();
					}
				}

			}
		} catch (Exception var18) {
			System.out.println(var18);
			return false;
		}
	}

	public void close() throws Exception {
		this.logger.info("Start closing connection pool...");
		this.pool.close();
		this.pool = null;
		this.logger.info("Connection pool closed.");
	}

	public void connect() throws Exception {
		this.logger.info("Start connecting...");
		List shards = this.hosts.stream().map((host) -> {
			String hostURI = String.format("redis://%s:%d/%d", host, 6379, this.databaseID);
			JedisShardInfo si = new JedisShardInfo(hostURI);
			si.setPassword(null);
			si.setConnectionTimeout(36000);
			return si;
		}).collect(Collectors.toList());
		this.logger.info("Shards:" + shards);
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		jedisPoolConfig.setMaxTotal(this.poolSize);
		this.pool = new ShardedJedisPool(jedisPoolConfig, shards);
		this.logger.info("Connection established.");
	}

	public void clearDB() throws Exception {
		CompletableFuture.allOf(this.hosts
			.stream()
			.map((host) -> CompletableFuture.runAsync(() -> {
				Jedis jedis = new Jedis(host, 6379, 36000);
				jedis.select(this.databaseID);
				jedis.flushDB();
				jedis.close();
			}))
			.toArray(CompletableFuture[]::new)).get();
	}
}
