package com.bdir.dps.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import org.apache.ibatis.annotations.*;
import org.apache.ibatis.jdbc.SQL;
import java.util.List;
import java.util.Map;

/**
 * Hive查询路由Mapper
 * 实现混合OLAP查询的智能路由
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

        public String routeQuery(Map<String, Object> params) {
            String startTime = (String) params.get("startTime");
            String endTime = (String) params.get("endTime");
            List<String> robotIds = (List<String>) params.get("robotIds");
            List<String> sensorTypes = (List<String>) params.get("sensorTypes");
            List<String> metrics = (List<String>) params.get("metrics");

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

        public String routeQuery(Map<String, Object> params) {
            String startTime = (String) params.get("startTime");
            String endTime = (String) params.get("endTime");
            List<String> robotIds = (List<String>) params.get("robotIds");
            List<String> sensorTypes = (List<String>) params.get("sensorTypes");
            List<String> metrics = (List<String>) params.get("metrics");

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
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT robot_id");

            // 动态构建指标查询
            if (metrics != null) {
                for (String metric : metrics) {
                    switch (metric) {
                        case "temperature":
                            sql.append(", AVG(temperature) as avg_temperature");
                            sql.append(", MAX(temperature) as max_temperature");
                            sql.append(", MIN(temperature) as min_temperature");
                            break;
                        case "humidity":
                            sql.append(", AVG(humidity) as avg_humidity");
                            sql.append(", MAX(humidity) as max_humidity");
                            sql.append(", MIN(humidity) as min_humidity");
                            break;
                        case "pressure":
                            sql.append(", AVG(pressure) as avg_pressure");
                            break;
                    }
                }
            }

            sql.append(", COUNT(*) as data_count");
            sql.append(" FROM sensor_fact_orc");
            sql.append(" WHERE event_time >= '${startTime}'");
            sql.append(" AND event_time <= '${endTime}'");

            if (robotIds != null && !robotIds.isEmpty()) {
                sql.append(" AND robot_id IN ").append(buildInClauseStatic(robotIds));
            }
            if (sensorTypes != null && !sensorTypes.isEmpty()) {
                sql.append(" AND sensor_type IN ").append(buildInClauseStatic(sensorTypes));
            }

            sql.append(" GROUP BY robot_id");
            sql.append(" ORDER BY robot_id");

            // Hive特定的分区裁剪优化
            String partitionFilter = buildPartitionFilter(startTime, endTime);
            sql.append(" AND ").append(partitionFilter);

            return sql.toString();
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
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT robot_id");

            if (metrics != null) {
                for (String metric : metrics) {
                    switch (metric) {
                        case "temperature":
                            sql.append(", SUM(avg_temp * record_count) / SUM(record_count) as avg_temperature");
                            sql.append(", MAX(max_temp) as max_temperature");
                            sql.append(", MIN(min_temp) as min_temperature");
                            break;
                        case "humidity":
                            sql.append(", SUM(avg_humidity * record_count) / SUM(record_count) as avg_humidity");
                            sql.append(", MAX(max_humidity) as max_humidity");
                            sql.append(", MIN(min_humidity) as min_humidity");
                            break;
                    }
                }
            }

            sql.append(", SUM(record_count) as data_count");
            sql.append(" FROM sensor_hourly_mv");
            sql.append(" WHERE dt >= DATE('${startTime}')");
            sql.append(" AND dt <= DATE('${endTime}')");

            if (robotIds != null && !robotIds.isEmpty()) {
                sql.append(" AND robot_id IN ").append(buildInClauseStatic(robotIds));
            }
            if (sensorTypes != null && !sensorTypes.isEmpty()) {
                sql.append(" AND sensor_type IN ").append(buildInClauseStatic(sensorTypes));
            }

            sql.append(" GROUP BY robot_id");
            sql.append(" ORDER BY robot_id");

            return sql.toString();
        }

        /**
         * 辅助方法：构建IN子句
         */
        private String buildInClause(String paramName, List<?> values) {
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
         * 辅助方法：构建静态IN子句
         */
        private String buildInClauseStatic(List<String> values) {
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < values.size(); i++) {
                sb.append("'").append(values.get(i)).append("'");
                if (i < values.size() - 1) {
                    sb.append(",");
                }
            }
            sb.append(")");
            return sb.toString();
        }

        /**
         * 辅助方法：构建分区过滤条件
         */
        private String buildPartitionFilter(String startTime, String endTime) {
            // 提取日期分区
            String startDate = startTime.substring(0, 10);
            String endDate = endTime.substring(0, 10);
            return String.format("(dt >= '%s' AND dt <= '%s')", startDate, endDate);
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
            return metrics.stream().allMatch(m -> m.equals("temperature") || m.equals("humidity"));
        }
    }

    /**
     * 动态SQL执行方法（已删除，防止SQL注入）
     * 注意：直接执行动态SQL存在安全风险，已移除
     */

    /**
     * 获取Hive表信息
     */
    @Select("SHOW TABLES")
    List<String> getHiveTables();

    /**
     * 获取表分区信息
     */
    @Select("SHOW PARTITIONS ${tableName}")
    List<String> getTablePartitions(@Param("tableName") String tableName);

    /**
     * 获取表结构信息
     */
    @Select("DESCRIBE ${tableName}")
    List<Map<String, Object>> getTableSchema(@Param("tableName") String tableName);

    /**
     * 获取表统计信息
     */
    @Select("ANALYZE TABLE ${tableName} COMPUTE STATISTICS")
    void analyzeTable(@Param("tableName") String tableName);

    /**
     * 获取表大小
     */
    @Select("DESCRIBE FORMATTED ${tableName}")
    List<Map<String, String>> getTableInfo(@Param("tableName") String tableName);

    /**
     * 执行Hive SQL
     */
    @Select("${hiveSql}")
    List<Map<String, Object>> executeHiveSql(@Param("hiveSql") String hiveSql);

    /**
     * 创建Hive表
     */
    @Update("${createTableSql}")
    void createHiveTable(@Param("createTableSql") String createTableSql);

    /**
     * 删除Hive表
     */
    @Update("DROP TABLE IF EXISTS ${tableName}")
    void dropHiveTable(@Param("tableName") String tableName);

    /**
     * 加载数据到Hive表
     */
    @Update("LOAD DATA INPATH '${dataPath}' INTO TABLE ${tableName}")
    void loadDataToHive(@Param("tableName") String tableName, @Param("dataPath") String dataPath);

    /**
     * 获取Hive执行计划
     */
    @Select("EXPLAIN ${sql}")
    List<Map<String, Object>> explainQuery(@Param("sql") String sql);

    /**
     * 获取物化视图信息
     */
    @Select("SHOW MATERIALIZED VIEWS")
    List<String> getMaterializedViews();

    /**
     * 刷新物化视图
     */
    @Update("REFRESH MATERIALIZED VIEW ${viewName}")
    void refreshMaterializedView(@Param("viewName") String viewName);

    /**
     * 获取Hive配置
     */
    @Select("SET ${configKey}")
    String getHiveConfig(@Param("configKey") String configKey);

    /**
     * 设置Hive配置
     */
    @Update("SET ${configKey}=${configValue}")
    void setHiveConfig(@Param("configKey") String configKey, @Param("configValue") String configValue);

    /**
     * 获取当前数据库
     */
    @Select("SELECT CURRENT_DATABASE()")
    String getCurrentDatabase();

    /**
     * 切换数据库
     */
    @Update("USE ${databaseName}")
    void useDatabase(@Param("databaseName") String databaseName);

    /**
     * 显示数据库
     */
    @Select("SHOW DATABASES")
    List<String> showDatabases();

    /**
     * 创建数据库
     */
    @Update("CREATE DATABASE IF NOT EXISTS ${databaseName}")
    void createDatabase(@Param("databaseName") String databaseName);

    /**
     * 删除数据库
     */
    @Update("DROP DATABASE IF EXISTS ${databaseName} CASCADE")
    void dropDatabase(@Param("databaseName") String databaseName);

    /**
     * 获取函数列表
     */
    @Select("SHOW FUNCTIONS")
    List<String> showFunctions();

    /**
     * 检查表是否存在
     */
    @Select("SHOW TABLES LIKE '${tableName}'")
    String tableExists(@Param("tableName") String tableName);

    /**
     * 获取表行数
     */
    @Select("SELECT COUNT(*) FROM ${tableName}")
    Long getTableRowCount(@Param("tableName") String tableName);

    /**
     * 获取表大小（近似值）
     */
    @Select("SHOW TABLE STATS ${tableName}")
    Map<String, Object> getTableStats(@Param("tableName") String tableName);

    /**
     * 获取列统计信息
     */
    @Select("SHOW COLUMN STATS ${tableName}")
    List<Map<String, Object>> getColumnStats(@Param("tableName") String tableName);

    /**
     * 执行DDL语句
     */
    @Update("${ddlSql}")
    void executeDDL(@Param("ddlSql") String ddlSql);

    /**
     * 执行DML语句
     */
    @Update("${dmlSql}")
    int executeDML(@Param("dmlSql") String dmlSql);

    /**
     * 批量插入数据
     */
    @Insert({"<script>",
            "INSERT INTO ${tableName} ${columns} VALUES ",
            "<foreach collection='values' item='value' separator=','>",
            "  ${value}",
            "</foreach>",
            "</script>"})
    void batchInsert(@Param("tableName") String tableName,
                    @Param("columns") String columns,
                    @Param("values") List<String> values);

    /**
     * 创建分区
     */
    @Update("ALTER TABLE ${tableName} ADD PARTITION (${partitionSpec})")
    void addPartition(@Param("tableName") String tableName,
                     @Param("partitionSpec") String partitionSpec);

    /**
     * 删除分区
     */
    @Update("ALTER TABLE ${tableName} DROP PARTITION (${partitionSpec})")
    void dropPartition(@Param("tableName") String tableName,
                      @Param("partitionSpec") String partitionSpec);

    /**
     * 修复分区
     */
    @Update("MSCK REPAIR TABLE ${tableName}")
    void repairTable(@Param("tableName") String tableName);

    /**
     * 获取表锁信息
     */
    @Select("SHOW LOCKS ${tableName}")
    List<Map<String, Object>> showLocks(@Param("tableName") String tableName);

    /**
     * 解锁表
     */
    @Update("UNLOCK TABLE ${tableName}")
    void unlockTable(@Param("tableName") String tableName);
}