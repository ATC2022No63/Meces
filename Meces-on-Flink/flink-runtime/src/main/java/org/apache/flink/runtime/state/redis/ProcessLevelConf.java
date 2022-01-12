package org.apache.flink.runtime.state.redis;

import org.apache.flink.configuration.ConfigConstants;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class ProcessLevelConf {
	private static final String CONF_FILE_NAME = "meces.conf.prop";
	private static Properties properties = null;

	//keys
	public static final String REDIS_HOSTS_KEY = "redis.hosts.list";
	public static final String REDIS_CONNECTION_POOL_SIZE_KEY = "redis.connection.pool.size";
	public static final String REDIS_DB_INDEX_KEY = "redis.db.index";
	public static final String KAFKA_HOSTS_KEY = "kafka.hosts.list";
	public static final String KAFKA_REQUEST_MESSAGE_TOPIC_KEY = "kafka.requestMessage.topic";
	public static final String KAFKA_STATE_TOPIC_KEY = "kafka.state.topic";
	public static final String KAFKA_STATE_INFO_TOPIC_KEY = "kafka.stateInfo.topic";
	public static final String TEST_REDIS_READING_COST_MILLI_KEY = "test.redis.reading.cost.milli";
	public static final String TEST_REDIS_WRITING_COST_MILLI_KEY = "test.redis.writing.cost.milli";
	public static final String TEST_PARTIAL_PAUSE = "test.partialPause";
	public static final String STATE_MAX_FETCH_TIMES_KEY = "state.maxFetchTimes";
	public static final String STATE_FETCH_INTERVAL_MILLI = "state.fetchInterval.milli";
	public static final String STATE_NUM_BINS = "state.numBins";
	public static final String STATE_BATCH_SIZE = "state.batchSize";
	public static final String COMMON_USE_ROUTE_TABLE = "common.useRouteTable";
	public static final String COMMON_POST_FETCH_ENABLED = "common.postFetchEnabled";
	public static final String COMMON_USE_POOLED_MAP = "common.usePooledMap";
	public static final String COMMON_POOLED_MAP_POOL_SIZE_PER_THREAD = "common.pooledMapPoolSizePerThread";
	public static final String OUTPUT_INFO = "outputInfo";

	public ProcessLevelConf() {
	}

	public static Properties getMecesConf() {
		return properties;
	}

	static {
		try {
			System.out.println(System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR));
			final File confFile = new File(System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR), CONF_FILE_NAME);
			FileInputStream inputStream = new FileInputStream(confFile);
			properties = new Properties();
			properties.load(inputStream);
		} catch (Exception var1) {
			var1.printStackTrace();
		}

	}

	public static String getProperty(String key) {
		return properties.getProperty(key);
	}
}
