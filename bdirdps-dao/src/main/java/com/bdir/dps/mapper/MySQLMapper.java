package com.bdir.dps.mapper;

import com.bdir.dps.entity.SensorData;
import com.bdir.dps.entity.Robot;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;

/**
 * MySQL数据访问层
 * 负责实时数据的增删改查操作
 */
@Repository
public class MySQLMapper {

    @Autowired
    private JdbcTemplate mysqlJdbcTemplate;

    /**
     * 插入传感器数据
     */
    public int insertSensorData(SensorData sensorData) {
        String sql = "INSERT INTO realtime_sensor_data " +
                "(data_id, robot_id, sensor_id, sensor_type, timestamp, " +
                "temperature, humidity, pressure, position_x, position_y, position_z, status) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        return mysqlJdbcTemplate.update(sql,
                sensorData.getDataId(),
                sensorData.getRobotId(),
                sensorData.getSensorId(),
                sensorData.getSensorType(),
                Timestamp.valueOf(sensorData.getTimestamp()),
                sensorData.getMetrics().get("temperature"),
                sensorData.getMetrics().get("humidity"),
                sensorData.getMetrics().get("pressure"),
                sensorData.getMetrics().get("position_x"),
                sensorData.getMetrics().get("position_y"),
                sensorData.getMetrics().get("position_z"),
                sensorData.getStatus()
        );
    }

    /**
     * 批量插入传感器数据
     */
    public int batchInsertSensorData(List<SensorData> sensorDataList) {
        String sql = "INSERT INTO realtime_sensor_data " +
                "(data_id, robot_id, sensor_id, sensor_type, timestamp, " +
                "temperature, humidity, pressure, position_x, position_y, position_z, status) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        int[] updateCounts = mysqlJdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                SensorData data = sensorDataList.get(i);
                ps.setString(1, data.getDataId());
                ps.setString(2, data.getRobotId());
                ps.setString(3, data.getSensorId());
                ps.setString(4, data.getSensorType());
                ps.setTimestamp(5, Timestamp.valueOf(data.getTimestamp()));
                ps.setDouble(6, data.getMetrics().get("temperature"));
                ps.setDouble(7, data.getMetrics().get("humidity"));
                ps.setDouble(8, data.getMetrics().get("pressure"));
                ps.setDouble(9, data.getMetrics().get("position_x"));
                ps.setDouble(10, data.getMetrics().get("position_y"));
                ps.setDouble(11, data.getMetrics().get("position_z"));
                ps.setString(12, data.getStatus());
            }

            @Override
            public int getBatchSize() {
                return sensorDataList.size();
            }
        });

        return Arrays.stream(updateCounts).sum();
    }

    /**
     * 查询实时传感器数据
     */
    public List<Map<String, Object>> queryRealtimeSensorData(List<String> robotIds,
                                                            List<String> sensorTypes,
                                                            int limit) {
        StringBuilder sql = new StringBuilder("SELECT * FROM realtime_sensor_data WHERE 1=1");
        List<Object> params = new ArrayList<>();

        if (robotIds != null && !robotIds.isEmpty()) {
            sql.append(" AND robot_id IN (");
            for (int i = 0; i < robotIds.size(); i++) {
                sql.append(i == 0 ? "?" : ", ?");
                params.add(robotIds.get(i));
            }
            sql.append(")");
        }

        if (sensorTypes != null && !sensorTypes.isEmpty()) {
            sql.append(" AND sensor_type IN (");
            for (int i = 0; i < sensorTypes.size(); i++) {
                sql.append(i == 0 ? "?" : ", ?");
                params.add(sensorTypes.get(i));
            }
            sql.append(")");
        }

        sql.append(" ORDER BY timestamp DESC LIMIT ?");
        params.add(limit);

        return mysqlJdbcTemplate.queryForList(sql.toString(), params.toArray());
    }

    /**
     * 查询机器人信息
     */
    public List<Robot> queryRobotInfo(List<String> robotIds) {
        StringBuilder sql = new StringBuilder("SELECT * FROM dim_robot WHERE 1=1");
        List<Object> params = new ArrayList<>();

        if (robotIds != null && !robotIds.isEmpty()) {
            sql.append(" AND robot_id IN (");
            for (int i = 0; i < robotIds.size(); i++) {
                sql.append(i == 0 ? "?" : ", ?");
                params.add(robotIds.get(i));
            }
            sql.append(")");
        }

        sql.append(" AND status = 1");

        return mysqlJdbcTemplate.query(sql.toString(), params.toArray(), new RobotRowMapper());
    }

    /**
     * 更新机器人状态
     */
    public int updateRobotStatus(String robotId, String status, String location, LocalDateTime updateTime) {
        String sql = "UPDATE dim_robot SET status = ?, location = ?, update_time = ? WHERE robot_id = ?";
        return mysqlJdbcTemplate.update(sql, status, location, Timestamp.valueOf(updateTime), robotId);
    }

    /**
     * 按时间范围查询传感器数据
     */
    public List<Map<String, Object>> querySensorDataByTimeRange(LocalDateTime startTime,
                                                               LocalDateTime endTime,
                                                               List<String> robotIds,
                                                               List<String> sensorTypes) {
        StringBuilder sql = new StringBuilder("SELECT * FROM realtime_sensor_data ");
        sql.append("WHERE timestamp BETWEEN ? AND ?");
        List<Object> params = new ArrayList<>();
        params.add(Timestamp.valueOf(startTime));
        params.add(Timestamp.valueOf(endTime));

        if (robotIds != null && !robotIds.isEmpty()) {
            sql.append(" AND robot_id IN (");
            for (int i = 0; i < robotIds.size(); i++) {
                sql.append(i == 0 ? "?" : ", ?");
                params.add(robotIds.get(i));
            }
            sql.append(")");
        }

        if (sensorTypes != null && !sensorTypes.isEmpty()) {
            sql.append(" AND sensor_type IN (");
            for (int i = 0; i < sensorTypes.size(); i++) {
                sql.append(i == 0 ? "?" : ", ?");
                params.add(sensorTypes.get(i));
            }
            sql.append(")");
        }

        sql.append(" ORDER BY timestamp DESC");

        return mysqlJdbcTemplate.queryForList(sql.toString(), params.toArray());
    }

    /**
     * 聚合查询传感器数据
     */
    public List<Map<String, Object>> queryAggregatedSensorData(LocalDateTime startTime,
                                                              LocalDateTime endTime,
                                                              String aggregationType,
                                                              List<String> robotIds) {
        String timeFormat;
        switch (aggregationType.toLowerCase()) {
            case "hourly":
                timeFormat = "%Y-%m-%d %H:00:00";
                break;
            case "daily":
                timeFormat = "%Y-%m-%d";
                break;
            case "weekly":
                timeFormat = "%x-%v";
                break;
            default:
                timeFormat = "%Y-%m-%d %H:%i:00";
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT DATE_FORMAT(timestamp, ?) as time_bucket, ");
        sql.append("robot_id, ");
        sql.append("COUNT(*) as data_count, ");
        sql.append("AVG(temperature) as avg_temperature, ");
        sql.append("MAX(temperature) as max_temperature, ");
        sql.append("MIN(temperature) as min_temperature, ");
        sql.append("AVG(humidity) as avg_humidity, ");
        sql.append("MAX(humidity) as max_humidity, ");
        sql.append("MIN(humidity) as min_humidity, ");
        sql.append("AVG(pressure) as avg_pressure, ");
        sql.append("MAX(pressure) as max_pressure, ");
        sql.append("MIN(pressure) as min_pressure ");
        sql.append("FROM realtime_sensor_data ");
        sql.append("WHERE timestamp BETWEEN ? AND ? ");

        List<Object> params = new ArrayList<>();
        params.add(timeFormat);
        params.add(Timestamp.valueOf(startTime));
        params.add(Timestamp.valueOf(endTime));

        if (robotIds != null && !robotIds.isEmpty()) {
            sql.append("AND robot_id IN (");
            for (int i = 0; i < robotIds.size(); i++) {
                sql.append(i == 0 ? "?" : ", ?");
                params.add(robotIds.get(i));
            }
            sql.append(") ");
        }

        sql.append("GROUP BY DATE_FORMAT(timestamp, ?), robot_id ");
        sql.append("ORDER BY time_bucket DESC, robot_id");
        params.add(timeFormat);

        return mysqlJdbcTemplate.queryForList(sql.toString(), params.toArray());
    }

    /**
     * 删除过期数据
     */
    public int deleteSensorData(LocalDateTime endTime) {
        String sql = "DELETE FROM realtime_sensor_data WHERE timestamp < ?";
        return mysqlJdbcTemplate.update(sql, Timestamp.valueOf(endTime));
    }

    /**
     * Robot实体类RowMapper
     */
    public static class RobotRowMapper implements RowMapper<Robot> {
        @Override
        public Robot mapRow(ResultSet rs, int rowNum) throws SQLException {
            Robot robot = new Robot();
            robot.setRobotId(rs.getString("robot_id"));
            robot.setRobotName(rs.getString("robot_name"));
            robot.setRobotType(rs.getString("robot_type"));
            robot.setModel(rs.getString("model"));
            robot.setStatus(rs.getString("status"));
            robot.setLocation(rs.getString("location"));
            robot.setDepartment(rs.getString("department"));
            robot.setResponsibleUser(rs.getString("responsible_user"));

            Timestamp createTime = rs.getTimestamp("create_time");
            if (createTime != null) {
                robot.setCreateTime(createTime.toLocalDateTime());
            }

            Timestamp updateTime = rs.getTimestamp("update_time");
            if (updateTime != null) {
                robot.setUpdateTime(updateTime.toLocalDateTime());
            }

            return robot;
        }
    }
}