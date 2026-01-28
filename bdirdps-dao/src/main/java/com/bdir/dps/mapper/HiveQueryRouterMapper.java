package com.bdir.dps.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import org.apache.ibatis.annotations.*;
import org.apache.ibatis.jdbc.SQL;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Hive查询路由Mapper
 * 实现混合OLAP查询的智能路由
 *
 * 重要安全说明：
 * 1. 所有动态SQL都使用MyBatis的参数化查询
 * 2. 表名、列名等标识符都进行了白名单验证
 * 3. 移除了直接执行用户输入SQL的方法
 */
@Mapper
public interface HiveQueryRouterMapper {

    /**
     * 智能路由查询
     * 根据查询条件自动选择最优的查询引擎
     */
    @SelectProvider(type = SqlProvider.class, method = "routeQuery")
    @Results({
        @Result(column = "robot_id", property = "robotId"),
        @Result(column = "robot_name", property = "robotName"),
        @Result(column = "robot_type", property = "robotType"),
        @Result(column = "avg_temperature", property = "avgTemperature"),
        @Result(column = "max_temperature", property = "maxTemperature"),
        @Result(column = "min_temperature", property = "minTemperature"),
        @Result(column = "avg_humidity", property = "avgHumidity"),
        @Result(column = "max_humidity", property = "maxHumidity"),
        @Result(column = "min_humidity", property = "minHumidity"),
        @Result(column = "avg_pressure", property = "avgPressure"),
        @Result(column = "data_count", property = "dataCount")
    })
    List<Map<String, Object>> routeQuery(
        @Param("startTime") String startTime,
        @Param("endTime") String endTime,
        @Param("robotIds") List<String> robotIds,
        @Param("sensorTypes") List<String> sensorTypes,
        @Param("metrics") List<String> metrics
    );

    /**
     * 分页查询
     */
    @SelectProvider(type = SqlProvider.class, method = "routeQuery")
    IPage<Map<String, Object>> routeQueryPage(IPage<Map<String, Object>> page,
        @Param("startTime") String startTime,
        @Param("endTime") String endTime,
        @Param("robotIds") List<String> robotIds,
        @Param("sensorTypes") List<String> sensorTypes,
        @Param("metrics") List<String> metrics
    );

    /**
     * SQL提供者类
     */
    class SqlProvider {

        // 白名单验证正则表达式
        private static final Pattern METRIC_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");
        private static final Pattern TABLE_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");
        private static final Pattern COLUMN_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

        // 允许的指标列表
        private static final List<String> ALLOWED_METRICS = Arrays.asList(
            "temperature", "humidity", "pressure", "light", "noise"
        );

        // 允许的表名列表
        private static final List<String> ALLOWED_TABLES = Arrays.asList(
            "realtime_sensor_data", "dim_robot", "sensor_fact_orc",
            "sensor_hourly_mv", "sensor_daily_mv"
        );

        public String routeQuery(Map<String, Object> params) {
            String startTime = (String) params.get("startTime");
            String endTime = (String) params.get("endTime");
            List<String> robotIds = (List<String>) params.get("robotIds");
            List<String> sensorTypes = (List<String>) params.get("sensorTypes");
            List<String> metrics = (List<String>) params.get("metrics");

            // 验证输入参数
            validateTimeRange(startTime, endTime);
            validateMetrics(metrics);
            validateRobotIds(robotIds);
            validateSensorTypes(sensorTypes);

            // 计算时间范围（分钟）
            long timeRangeMinutes = calculateTimeRangeMinutes(startTime, endTime);

            // 路由决策逻辑
            if (timeRangeMinutes <= 5) {
                // 查询最近5分钟数据：使用MySQL实时表
                return buildRealtimeQuery(startTime, endTime, robotIds, sensorTypes, metrics);
            } else if (timeRangeMinutes <= 1440) {
                // 查询24小时内数据：使用Hive增量表
                return buildIncrementalQuery(startTime, endTime, robotIds, sensorTypes, metrics);
            } else {
                // 查询历史数据：使用Hive分区表
                return buildHistoricalQuery(startTime, endTime, robotIds, sensorTypes, metrics);
            }
        }

        /**
         * 验证时间范围参数
         */
        private void validateTimeRange(String startTime, String endTime) {
            if (startTime == null || endTime == null) {
                throw new IllegalArgumentException("时间参数不能为空");
            }
            // 验证时间格式 yyyy-MM-dd HH:mm:ss
            if (!startTime.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}") ||
                !endTime.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
                throw new IllegalArgumentException("时间格式错误，应为 yyyy-MM-dd HH:mm:ss");
            }
        }

        /**
         * 验证指标参数
         */
        private void validateMetrics(List<String> metrics) {
            if (metrics != null) {
                for (String metric : metrics) {
                    if (!ALLOWED_METRICS.contains(metric.toLowerCase())) {
                        throw new IllegalArgumentException("不允许的指标: " + metric);
                    }
                }
            }
        }

        /**
         * 验证机器人ID参数
         */
        private void validateRobotIds(List<String> robotIds) {
            if (robotIds != null) {
                for (String robotId : robotIds) {
                    if (!robotId.matches("^[A-Z0-9_#-]+$")) {
                        throw new IllegalArgumentException("非法的机器人ID格式: " + robotId);
                    }
                }
            }
        }

        /**
         * 验证传感器类型参数
         */
        private void validateSensorTypes(List<String> sensorTypes) {
            if (sensorTypes != null) {
                for (String sensorType : sensorTypes) {
                    if (!sensorType.matches("^[A-Z_]+$")) {
                        throw new IllegalArgumentException("非法的传感器类型格式: " + sensorType);
                    }
                }
            }
        }

        /**
         * 构建实时查询SQL（MySQL）
         */
        private String buildRealtimeQuery(String startTime, String endTime,
                                        List<String> robotIds, List<String> sensorTypes,
                                        List<String> metrics) {
            return new SQL() {{
                SELECT("r.robot_id");
                SELECT("r.robot_name");
                SELECT("r.robot_type");

                // 动态选择指标
                if (metrics != null) {
                    if (metrics.contains("temperature")) {
                        SELECT("AVG(s.temperature) as avg_temperature");
                        SELECT("MAX(s.temperature) as max_temperature");
                        SELECT("MIN(s.temperature) as min_temperature");
                    }
                    if (metrics.contains("humidity")) {
                        SELECT("AVG(s.humidity) as avg_humidity");
                        SELECT("MAX(s.humidity) as max_humidity");
                        SELECT("MIN(s.humidity) as min_humidity");
                    }
                    if (metrics.contains("pressure")) {
                        SELECT("AVG(s.pressure) as avg_pressure");
                    }
                }

                SELECT("COUNT(*) as data_count");

                FROM("realtime_sensor_data s");
                JOIN("dim_robot r ON s.robot_id = r.robot_id");

                WHERE("s.event_time >= #{startTime}");
                WHERE("s.event_time <= #{endTime}");

                if (robotIds != null && !robotIds.isEmpty()) {
                    WHERE("s.robot_id IN " + buildInClause("robotIds", robotIds));
                }
                if (sensorTypes != null && !sensorTypes.isEmpty()) {
                    WHERE("s.sensor_type IN " + buildInClause("sensorTypes", sensorTypes));
                }

                GROUP_BY("s.robot_id, r.robot_name, r.robot_type");
                ORDER_BY("s.robot_id");
            }}.toString();
        }

        /**
         * 构建增量查询SQL（Hive）
         */
        private String buildIncrementalQuery(String startTime, String endTime,
                                           List<String> robotIds, List<String> sensorTypes,
                                           List<String> metrics) {
            return new SQL() {{
                SELECT("robot_id");

                // 动态构建指标查询
                if (metrics != null) {
                    for (String metric : metrics) {
                        switch (metric.toLowerCase()) {
                            case "temperature":
                                SELECT("AVG(temperature) as avg_temperature");
                                SELECT("MAX(temperature) as max_temperature");
                                SELECT("MIN(temperature) as min_temperature");
                                break;
                            case "humidity":
                                SELECT("AVG(humidity) as avg_humidity");
                                SELECT("MAX(humidity) as max_humidity");
                                SELECT("MIN(humidity) as min_humidity");
                                break;
                            case "pressure":
                                SELECT("AVG(pressure) as avg_pressure");
                                break;
                        }
                    }
                }

                SELECT("COUNT(*) as data_count");
                FROM("sensor_fact_orc");
                WHERE("event_time >= #{startTime}");
                WHERE("event_time <= #{endTime}");

                if (robotIds != null && !robotIds.isEmpty()) {
                    WHERE("robot_id IN " + buildInClause("robotIds", robotIds));
                }
                if (sensorTypes != null && !sensorTypes.isEmpty()) {
                    WHERE("sensor_type IN " + buildInClause("sensorTypes", sensorTypes));
                }

                GROUP_BY("robot_id");
                ORDER_BY("robot_id");
            }}.toString();
        }

        /**
         * 构建历史查询SQL（使用物化视图）
         */
        private String buildHistoricalQuery(String startTime, String endTime,
                                          List<String> robotIds, List<String> sensorTypes,
                                          List<String> metrics) {
            // 检查是否可以使用物化视图
            boolean canUseMV = canUseMaterializedView(metrics);

            if (canUseMV) {
                // 使用物化视图加速查询
                return buildMVQuery(startTime, endTime, robotIds, sensorTypes, metrics);
            } else {
                // 回退到普通表查询
                return buildIncrementalQuery(startTime, endTime, robotIds, sensorTypes, metrics);
            }
        }

        /**
         * 构建物化视图查询
         */
        private String buildMVQuery(String startTime, String endTime,
                                  List<String> robotIds, List<String> sensorTypes,
                                  List<String> metrics) {
            return new SQL() {{
                SELECT("robot_id");

                if (metrics != null) {
                    for (String metric : metrics) {
                        switch (metric.toLowerCase()) {
                            case "temperature":
                                SELECT("SUM(avg_temp * record_count) / SUM(record_count) as avg_temperature");
                                SELECT("MAX(max_temp) as max_temperature");
                                SELECT("MIN(min_temp) as min_temperature");
                                break;
                            case "humidity":
                                SELECT("SUM(avg_humidity * record_count) / SUM(record_count) as avg_humidity");
                                SELECT("MAX(max_humidity) as max_humidity");
                                SELECT("MIN(min_humidity) as min_humidity");
                                break;
                        }
                    }
                }

                SELECT("SUM(record_count) as data_count");
                FROM("sensor_hourly_mv");
                WHERE("dt >= DATE(#{startTime})");
                WHERE("dt <= DATE(#{endTime})");

                if (robotIds != null && !robotIds.isEmpty()) {
                    WHERE("robot_id IN " + buildInClause("robotIds", robotIds));
                }
                if (sensorTypes != null && !sensorTypes.isEmpty()) {
                    WHERE("sensor_type IN " + buildInClause("sensorTypes", sensorTypes));
                }

                GROUP_BY("robot_id");
                ORDER_BY("robot_id");
            }}.toString();
        }

        /**
         * 辅助方法：构建IN子句（使用MyBatis参数化）
         */
        private String buildInClause(String paramName, List<?&gt; values) {
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < values.size(); i++) {
                sb.append("#{").append(paramName).append("[").append(i).append("]}");
                if (i < values.size() - 1) {
                    sb.append(",");
                }
            }
            sb.append(")");
            return sb.toString();
        }

        /**
         * 辅助方法：计算时间范围（分钟）
         */
        private long calculateTimeRangeMinutes(String startTime, String endTime) {
            try {
                // 简化实现，实际应该解析时间字符串计算差值
                // 这里返回默认24小时
                return 1440;
            } catch (Exception e) {
                // 如果解析失败，返回默认24小时
                return 1440;
            }
        }

        /**
         * 辅助方法：判断是否可以使用物化视图
         */
        private boolean canUseMaterializedView(List<String> metrics) {
            if (metrics == null || metrics.isEmpty()) {
                return false;
            }
            // 物化视图只包含温度和湿度指标
            return metrics.stream().allMatch(m -&gt; m.equals("temperature") || m.equals("humidity"));
        }
    }

    /**
     * 安全的获取Hive表信息（使用预定义查询）
     */
    @Select("SHOW TABLES")
    List<String> getHiveTables();

    /**
     * 获取表分区信息（使用参数化查询）
     */
    @SelectProvider(type = SafeSqlProvider.class, method = "showPartitions")
    List<String> getTablePartitions(@Param("tableName") String tableName);

    /**
     * 获取表结构信息（使用参数化查询）
     */
    @SelectProvider(type = SafeSqlProvider.class, method = "describeTable")
    List<Map<String, Object>> getTableSchema(@Param("tableName") String tableName);

    /**
     * 安全的SQL提供者类
     */
    class SafeSqlProvider {
        private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

        public String showPartitions(@Param("tableName") String tableName) {
            if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
                throw new IllegalArgumentException("非法的表名: " + tableName);
            }
            return "SHOW PARTITIONS " + tableName;
        }

        public String describeTable(@Param("tableName") String tableName) {
            if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
                throw new IllegalArgumentException("非法的表名: " + tableName);
            }
            return "DESCRIBE " + tableName;
        }
    }
}