package org.apache.flink.runtime.state.replication;


import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class ProcessLevelConf {
    private static final String CONF_FILE_NAME = "rhino-conf";
    private static final String ENV_CONF = "CONF_DIR";
    private static Properties properties = null;

    //keys
    public static final String RESCLAE_TOPIC = "kafka.rescale.topic";
    public static final String REPLICATION_TOPIC = "kafka.replication.topic";
    public static final String REPLICATION_INTERVAL = "replication.interval";
    public static final String KAFKA_HOST = "kafka.host";
    public static final String ENABLE_REPLICATION = "replication.enable";
    public static final String KAFKA_TIMEOUT = "kafka.session.timeout";
    public static final String REPLICATION_PORT = "replication.port";
    public static final String NUM_INCREMENTAL = "replication.incremental.num";

    public ProcessLevelConf() {
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
        } catch (Exception var1) {
            var1.printStackTrace();
        }

    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}
