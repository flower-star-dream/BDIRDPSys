package com.bdir.dps.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Hive配置类
 * 配置Hive连接、数据源和JdbcTemplate
 */
@Configuration
public class HiveConfig {

    @Value("${hive.url:jdbc:hive2://localhost:10000/default}")
    private String hiveUrl;

    @Value("${hive.username:hive}")
    private String username;

    @Value("${hive.password:hive}")
    private String password;

    @Value("${hive.driver-class-name:org.apache.hive.jdbc.HiveDriver}")
    private String driverClassName;

    @Value("${hive.connection-timeout:30000}")
    private int connectionTimeout;

    @Value("${hive.max-pool-size:10}")
    private int maxPoolSize;

    @Value("${hive.min-idle:2}")
    private int minIdle;

    @Value("${hive.max-wait:60000}")
    private int maxWait;

    @Value("${hive.time-between-eviction-runs-millis:60000}")
    private int timeBetweenEvictionRunsMillis;

    @Value("${hive.min-evictable-idle-time-millis:300000}")
    private int minEvictableIdleTimeMillis;

    /**
     * 配置Hive数据源
     */
    @Bean(name = "hiveDataSource")
    @Primary
    public DataSource hiveDataSource() {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(hiveUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setDriverClassName(driverClassName);

        // 连接池配置
        dataSource.setMaxActive(maxPoolSize);
        dataSource.setMinIdle(minIdle);
        dataSource.setMaxWait(maxWait);
        dataSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        dataSource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);

        // 验证连接
        dataSource.setValidationQuery("SELECT 1");
        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnBorrow(false);
        dataSource.setTestOnReturn(false);

        // 连接超时配置
        Properties properties = new Properties();
        properties.setProperty("connectTimeout", String.valueOf(connectionTimeout));
        dataSource.setConnectProperties(properties);

        return dataSource;
    }

    /**
     * 配置Hive JdbcTemplate
     */
    @Bean(name = "hiveJdbcTemplate")
    public JdbcTemplate hiveJdbcTemplate(DataSource hiveDataSource) {
        return new JdbcTemplate(hiveDataSource);
    }

    /**
     * 配置Hive连接
     */
    @Bean
    public HiveConnectionProvider hiveConnectionProvider() {
        return new HiveConnectionProvider(hiveUrl, username, password);
    }

    /**
     * Hive连接提供器
     */
    public static class HiveConnectionProvider {
        private final String url;
        private final String username;
        private final String password;

        public HiveConnectionProvider(String url, String username, String password) {
            this.url = url;
            this.username = username;
            this.password = password;
        }

        public Connection getConnection() throws SQLException {
            return DriverManager.getConnection(url, username, password);
        }
    }

    /**
     * 配置Hive Metastore客户端
     */
    @Bean
    public HiveMetaStoreClient hiveMetaStoreClient() throws MetaException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:9083");
        return new HiveMetaStoreClient(hiveConf);
    }

    /**
     * Hive表配置
     */
    @Bean
    public HiveTableConfig hiveTableConfig() {
        return new HiveTableConfig();
    }

    /**
     * Hive表配置类
     */
    public static class HiveTableConfig {
        // 传感器数据表
        public static final String SENSOR_DATA_TABLE = "sensor_data";
        public static final String SENSOR_DATA_PARTITIONED_TABLE = "sensor_data_partitioned";
        public static final String SENSOR_DATA_BUCKETED_TABLE = "sensor_data_bucketed";

        // 机器人信息表
        public static final String ROBOT_INFO_TABLE = "robot_info";
        public static final String ROBOT_STATUS_TABLE = "robot_status";

        // 统计表
        public static final String SENSOR_STATISTICS_TABLE = "sensor_statistics";
        public static final String DAILY_AGGREGATION_TABLE = "daily_aggregation";
        public static final String HOURLY_AGGREGATION_TABLE = "hourly_aggregation";

        // 异常检测表
        public static final String ANOMALY_DETECTION_TABLE = "anomaly_detection";
        public static final String DATA_QUALITY_TABLE = "data_quality";

        // 分区配置
        public static final String PARTITION_BY_DAY = "dt";
        public static final String PARTITION_BY_HOUR = "hr";
        public static final String PARTITION_BY_ROBOT = "robot_id";

        // 分桶配置
        public static final int BUCKET_COUNT = 16;
        public static final String BUCKET_COLUMN = "sensor_id";

        // 存储格式
        public static final String STORAGE_FORMAT = "PARQUET";
        public static final String COMPRESSION_CODEC = "SNAPPY";

        // 表属性
        public static final String TABLE_PROPERTIES = "TBLPROPERTIES ('parquet.compression'='SNAPPY')";
    }

    /**
     * Hive查询配置
     */
    @Bean
    public HiveQueryConfig hiveQueryConfig() {
        return new HiveQueryConfig();
    }

    /**
     * Hive查询配置类
     */
    public static class HiveQueryConfig {
        // 查询超时时间（秒）
        public static final int QUERY_TIMEOUT = 300;

        // 最大返回记录数
        public static final int MAX_RESULTS = 10000;

        // 默认并行度
        public static final int DEFAULT_PARALLELISM = 4;

        // 动态分区配置
        public static final String DYNAMIC_PARTITION_MODE = "nonstrict";
        public static final String DYNAMIC_PARTITION_MAX_PARTS = "1000";

        // 执行引擎
        public static final String EXECUTION_ENGINE = "tez";

        // 优化配置
        public static final String OPTIMIZE_CONFIGS = "SET hive.exec.dynamic.partition=true; " +
                "SET hive.exec.dynamic.partition.mode=nonstrict; " +
                "SET hive.execution.engine=tez; " +
                "SET hive.vectorized.execution.enabled=true; " +
                "SET hive.vectorized.execution.reduce.enabled=true;";
    }

    /**
     * Hive分区策略
     */
    public enum PartitionStrategy {
        DAILY("dt", "按天分区"),
        HOURLY("dt,hr", "按小时分区"),
        ROBOT("robot_id", "按机器人分区"),
        COMBINED("dt,robot_id", "按天和机器人组合分区");

        private final String columns;
        private final String description;

        PartitionStrategy(String columns, String description) {
            this.columns = columns;
            this.description = description;
        }

        public String getColumns() {
            return columns;
        }

        public String getDescription() {
            return description;
        }
    }

    /**
     * Hive数据类型映射
     */
    public static class HiveDataType {
        public static final String STRING = "STRING";
        public static final String INT = "INT";
        public static final String BIGINT = "BIGINT";
        public static final String DOUBLE = "DOUBLE";
        public static final String DECIMAL = "DECIMAL(18,6)";
        public static final String TIMESTAMP = "TIMESTAMP";
        public static final String DATE = "DATE";
        public static final String BOOLEAN = "BOOLEAN";
        public static final String ARRAY_STRING = "ARRAY<STRING>";
        public static final String MAP_STRING_DOUBLE = "MAP<STRING,DOUBLE>";
        public static final String STRUCT = "STRUCT<";
    }
}

/**
 * Hive工具类
 */
class HiveUtils {
    /**
     * 构建创建表的SQL语句
     */
    public static String buildCreateTableSQL(String tableName, String columns, String partitionColumns, String bucketInfo, String storageFormat, String location) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE EXTERNAL TABLE IF NOT EXISTS ").append(tableName).append(" (\n");
        sql.append(columns).append("\n");
        sql.append(")\n");

        if (partitionColumns != null && !partitionColumns.isEmpty()) {
            sql.append("PARTITIONED BY (")").append(partitionColumns).append(")\n");
        }

        if (bucketInfo != null && !bucketInfo.isEmpty()) {
            sql.append("CLUSTERED BY ").append(bucketInfo).append("\n");
        }

        sql.append("STORED AS ").append(storageFormat).append("\n");

        if (location != null && !location.isEmpty()) {
            sql.append("LOCATION '").append(location).append("'\n");
        }

        sql.append("TBLPROPERTIES ('parquet.compression'='SNAPPY')");

        return sql.toString();
    }

    /**
     * 构建添加分区的SQL语句
     */
    public static String buildAddPartitionSQL(String tableName, String partitionSpec, String location) {
        return String.format("ALTER TABLE %s ADD PARTITION (%s) LOCATION '%s'", tableName, partitionSpec, location);
    }

    /**
     * 构建删除分区的SQL语句
     */
    public static String buildDropPartitionSQL(String tableName, String partitionSpec) {
        return String.format("ALTER TABLE %s DROP PARTITION (%s)", tableName, partitionSpec);
    }

    /**
     * 构建MSCK修复语句
     */
    public static String buildMSCKSQL(String tableName) {
        return String.format("MSCK REPAIR TABLE %s", tableName);
    }
}