package org.apache.flink.runtime.microbatch;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class ProcessConf {
    private static final String CONF_FILE_NAME = "megaphone-conf";
    private static final String ENV_CONF = "CONF_DIR";
    private static Properties properties;

    //keys
    public static final String SYNC_TOPIC = "kafka.sync.topic";
    public static final String FETCH_TOPIC = "fetch.topic";
    public static final String ACK_TOPIC = "ack.topic";
    public static final String KAFKA_HOST = "kafka.host";
    public static final String KAFKA_TIMEOUT = "kafka.session.timeout";
    public static final String BATCH_ENABLED = "batch.enabled";
    public static final String BATCH_INTERVAL = "batch.interval";
    public static final String MIGRATION_PORT = "migration.port";
    public static final String NUM_SINK = "numberOfSink";
    public static final String RESCALE_TASK_NAME = "rescale.task.name";
    public static final String UPSTREAM_TASK_NAME = "upstream.task.name";
    public static final String MAGRATION_NUM = "migration.number";

    public ProcessConf() {
    }

    public static Properties getPasaConf() {
        return properties;
    }

    static {
        try {
            System.out.println(System.getenv(ENV_CONF));
            final File confFile = new File(System.getenv(ENV_CONF), CONF_FILE_NAME);
            if (!confFile.exists())
                System.out.printf("error:no conf file!\n");
            FileInputStream inputStream = new FileInputStream(confFile);
            properties = new Properties();
            properties.load(inputStream);
            System.out.printf("topic:%s", properties.getProperty(SYNC_TOPIC));
        } catch (Exception var1) {
            var1.printStackTrace();
        }

    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}
