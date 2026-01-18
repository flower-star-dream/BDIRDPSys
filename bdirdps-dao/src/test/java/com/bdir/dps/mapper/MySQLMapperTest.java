package com.bdir.dps.mapper;

import com.bdir.dps.entity.SensorData;
import com.bdir.dps.entity.Robot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * MySQL Mapper测试类
 */
@ExtendWith(MockitoExtension.class)
class MySQLMapperTest {

    @Mock
    private JdbcTemplate mysqlJdbcTemplate;

    @Mock
    private ResultSet resultSet;

    private MySQLMapper mySQLMapper;

    @BeforeEach
    void setUp() {
        mySQLMapper = new MySQLMapper();
        mySQLMapper.setMysqlJdbcTemplate(mysqlJdbcTemplate);
    }

    /**
     * 测试插入传感器数据
     */
    @Test
    void testInsertSensorData() {
        // 准备测试数据
        SensorData sensorData = new SensorData();
        sensorData.setDataId("test-123");
        sensorData.setRobotId("R001");
        sensorData.setSensorId("S001");
        sensorData.setSensorType("TEMPERATURE");
        sensorData.setTimestamp(LocalDateTime.now());

        Map<String, Double> metrics = new HashMap<>();
        metrics.put("temperature", 25.5);
        metrics.put("humidity", 60.0);
        sensorData.setMetrics(metrics);

        // 设置Mock行为
        when(mysqlJdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);

        // 执行插入
        int result = mySQLMapper.insertSensorData(sensorData);

        // 验证结果
        assertEquals(1, result);
        verify(mysqlJdbcTemplate, times(1)).update(anyString(), any(Object[].class));
    }

    /**
     * 测试批量插入传感器数据
     */
    @Test
    void testBatchInsertSensorData() {
        // 准备测试数据
        List<SensorData> sensorDataList = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            SensorData sensorData = new SensorData();
            sensorData.setDataId("test-" + i);
            sensorData.setRobotId("R001");
            sensorData.setSensorId("S001");
            sensorData.setSensorType("TEMPERATURE");
            sensorData.setTimestamp(LocalDateTime.now());

            Map<String, Double> metrics = new HashMap<>();
            metrics.put("temperature", 25.0 + i);
            sensorData.setMetrics(metrics);

            sensorDataList.add(sensorData);
        }

        // 设置Mock行为
        when(mysqlJdbcTemplate.batchUpdate(anyString(), any(List.class))).thenReturn(new int[]{5});

        // 执行批量插入
        int result = mySQLMapper.batchInsertSensorData(sensorDataList);

        // 验证结果
        assertEquals(5, result);
        verify(mysqlJdbcTemplate, times(1)).batchUpdate(anyString(), any(List.class));
    }

    /**
     * 测试查询实时传感器数据
     */
    @Test
    void testQueryRealtimeSensorData() {
        // 准备测试参数
        List<String> robotIds = Arrays.asList("R001", "R002");
        List<String> sensorTypes = Arrays.asList("TEMPERATURE", "HUMIDITY");
        int limit = 100;

        // 准备模拟结果
        List<Map<String, Object>> mockResults = new ArrayList<>();
        Map<String, Object> data1 = new HashMap<>();
        data1.put("data_id", "realtime-1");
        data1.put("robot_id", "R001");
        data1.put("sensor_id", "S001");
        data1.put("sensor_type", "TEMPERATURE");
        data1.put("timestamp", LocalDateTime.now());
        data1.put("temperature", 25.5);
        mockResults.add(data1);

        // 设置Mock行为
        when(mysqlJdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(mockResults);

        // 执行查询
        List<Map<String, Object>> results = mySQLMapper.queryRealtimeSensorData(robotIds, sensorTypes, limit);

        // 验证结果
        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals("realtime-1", results.get(0).get("data_id"));
        assertEquals("R001", results.get(0).get("robot_id"));
        assertEquals(25.5, results.get(0).get("temperature"));

        // 验证SQL执行
        verify(mysqlJdbcTemplate, times(1)).queryForList(anyString(), any(Object[].class));
    }

    /**
     * 测试查询机器人信息
     */
    @Test
    void testQueryRobotInfo() {
        // 准备测试参数
        List<String> robotIds = Arrays.asList("R001", "R002");

        // 准备模拟结果
        List<Robot> mockRobots = new ArrayList<>();
        Robot robot1 = new Robot();
        robot1.setRobotId("R001");
        robot1.setRobotName("机器人1号");
        robot1.setRobotType("AGV");
        robot1.setStatus("ONLINE");
        robot1.setLocation("车间A");
        mockRobots.add(robot1);

        // 设置Mock行为
        when(mysqlJdbcTemplate.query(anyString(), any(Object[].class), any(RowMapper.class)))
                .thenReturn(mockRobots);

        // 执行查询
        List<Robot> results = mySQLMapper.queryRobotInfo(robotIds);

        // 验证结果
        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals("R001", results.get(0).getRobotId());
        assertEquals("机器人1号", results.get(0).getRobotName());
        assertEquals("AGV", results.get(0).getRobotType());

        // 验证SQL执行
        verify(mysqlJdbcTemplate, times(1)).query(anyString(), any(Object[].class), any(RowMapper.class));
    }

    /**
     * 测试更新机器人状态
     */
    @Test
    void testUpdateRobotStatus() {
        // 准备测试参数
        String robotId = "R001";
        String status = "MAINTENANCE";
        String location = "维修区";
        LocalDateTime updateTime = LocalDateTime.now();

        // 设置Mock行为
        when(mysqlJdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);

        // 执行更新
        int result = mySQLMapper.updateRobotStatus(robotId, status, location, updateTime);

        // 验证结果
        assertEquals(1, result);
        verify(mysqlJdbcTemplate, times(1)).update(anyString(), any(Object[].class));
    }

    /**
     * 测试异常处理
     */
    @Test
    void testInsertSensorData_Exception() {
        // 准备测试数据
        SensorData sensorData = new SensorData();
        sensorData.setDataId("test-123");
        sensorData.setRobotId("R001");
        sensorData.setSensorId("S001");
        sensorData.setSensorType("TEMPERATURE");
        sensorData.setTimestamp(LocalDateTime.now());

        // 设置Mock行为（抛出异常）
        when(mysqlJdbcTemplate.update(anyString(), any(Object[].class)))
                .thenThrow(new RuntimeException("Database connection failed"));

        // 执行插入并验证异常
        assertThrows(RuntimeException.class, () -> {
            mySQLMapper.insertSensorData(sensorData);
        });
    }

    /**
     * 测试空参数处理
     */
    @Test
    void testQueryRealtimeSensorData_EmptyParams() {
        // 测试空参数
        List<String> emptyRobotIds = new ArrayList<>();
        List<String> emptySensorTypes = new ArrayList<>();

        // 准备模拟结果
        List<Map<String, Object>> mockResults = new ArrayList<>();

        // 设置Mock行为
        when(mysqlJdbcTemplate.queryForList(anyString(), any(Object[].class)))
                .thenReturn(mockResults);

        // 执行查询
        List<Map<String, Object>> results = mySQLMapper.queryRealtimeSensorData(
            emptyRobotIds, emptySensorTypes, 100
        );

        // 验证结果
        assertNotNull(results);
        assertTrue(results.isEmpty());

        // 验证SQL执行
        verify(mysqlJdbcTemplate, times(1)).queryForList(anyString(), any(Object[].class));
    }

    /**
     * 测试大数据量查询
     */
    @Test
    void testQueryRealtimeSensorData_LargeDataSet() {
        // 准备测试参数
        int limit = 1000;

        // 准备大量模拟结果
        List<Map<String, Object>> mockResults = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("data_id", "data-" + i);
            data.put("robot_id", "R" + String.format("%03d", i % 100));
            data.put("sensor_id", "S" + String.format("%03d", i % 50));
            data.put("sensor_type", "TEMPERATURE");
            data.put("timestamp", LocalDateTime.now().minusMinutes(i));
            data.put("temperature", 20.0 + i * 0.01);
            mockResults.add(data);
        }

        // 设置Mock行为
        when(mysqlJdbcTemplate.queryForList(anyString(), any(Object[].class)))
                .thenReturn(mockResults);

        // 执行查询
        List<Map<String, Object>> results = mySQLMapper.queryRealtimeSensorData(null, null, limit);

        // 验证结果
        assertNotNull(results);
        assertEquals(limit, results.size());
        assertEquals("data-0", results.get(0).get("data_id"));
        assertEquals("data-999", results.get(999).get("data_id"));
    }

    /**
     * 测试时间范围查询
     */
    @Test
    void testQuerySensorDataByTimeRange() {
        // 准备测试参数
        LocalDateTime startTime = LocalDateTime.now().minusHours(2);
        LocalDateTime endTime = LocalDateTime.now().minusHours(1);
        List<String> robotIds = Arrays.asList("R001");
        List<String> sensorTypes = Arrays.asList("TEMPERATURE");

        // 准备模拟结果
        List<Map<String, Object>> mockResults = new ArrayList<>();
        Map<String, Object> data = new HashMap<>();
        data.put("data_id", "time-range-1");
        data.put("robot_id", "R001");
        data.put("sensor_id", "S001");
        data.put("sensor_type", "TEMPERATURE");
        data.put("timestamp", startTime.plusMinutes(30));
        data.put("temperature", 25.5);
        mockResults.add(data);

        // 设置Mock行为
        when(mysqlJdbcTemplate.queryForList(anyString(), any(Object[].class)))
                .thenReturn(mockResults);

        // 执行查询
        List<Map<String, Object>> results = mySQLMapper.querySensorDataByTimeRange(
            startTime, endTime, robotIds, sensorTypes
        );

        // 验证结果
        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals("time-range-1", results.get(0).get("data_id"));

        // 验证SQL执行
        verify(mysqlJdbcTemplate, times(1)).queryForList(anyString(), any(Object[].class));
    }

    /**
     * 测试数据聚合查询
     */
    @Test
    void testQueryAggregatedSensorData() {
        // 准备测试参数
        LocalDateTime startTime = LocalDateTime.now().minusDays(1);
        LocalDateTime endTime = LocalDateTime.now();
        String aggregationType = "hourly";
        List<String> robotIds = Arrays.asList("R001", "R002");

        // 准备模拟结果
        List<Map<String, Object>> mockResults = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("hour", i);
            data.put("robot_id", "R001");
            data.put("avg_temperature", 25.0 + Math.sin(i) * 2);
            data.put("max_temperature", 27.0 + Math.sin(i) * 2);
            data.put("min_temperature", 23.0 + Math.sin(i) * 2);
            data.put("avg_humidity", 60.0 + Math.cos(i) * 5);
            data.put("data_count", 60L);
            mockResults.add(data);
        }

        // 设置Mock行为
        when(mysqlJdbcTemplate.queryForList(anyString(), any(Object[].class)))
                .thenReturn(mockResults);

        // 执行查询
        List<Map<String, Object>> results = mySQLMapper.queryAggregatedSensorData(
            startTime, endTime, aggregationType, robotIds
        );

        // 验证结果
        assertNotNull(results);
        assertEquals(24, results.size());
        assertEquals(0, results.get(0).get("hour"));
        assertEquals(23, results.get(23).get("hour"));
        assertTrue((Long) results.get(0).get("data_count") > 0);
    }

    /**
     * 测试数据删除
     */
    @Test
    void testDeleteSensorData() {
        // 准备测试参数
        LocalDateTime endTime = LocalDateTime.now().minusDays(90); // 删除90天前的数据

        // 设置Mock行为
        when(mysqlJdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1000);

        // 执行删除
        int result = mySQLMapper.deleteSensorData(endTime);

        // 验证结果
        assertEquals(1000, result);
        verify(mysqlJdbcTemplate, times(1)).update(anyString(), any(Object[].class));
    }

    /**
     * 测试事务回滚
     */
    @Test
    void testTransactionRollback() {
        // 准备测试数据
        List<SensorData> sensorDataList = new ArrayList<>();

        SensorData sensorData1 = new SensorData();
        sensorData1.setDataId("test-1");
        sensorData1.setRobotId("R001");
        sensorData1.setSensorId("S001");
        sensorData1.setSensorType("TEMPERATURE");
        sensorData1.setTimestamp(LocalDateTime.now());
        sensorDataList.add(sensorData1);

        SensorData sensorData2 = new SensorData();
        sensorData2.setDataId("test-2");
        sensorData2.setRobotId("R002");
        sensorData2.setSensorId(null); // 无效数据，导致异常
        sensorData2.setSensorType("HUMIDITY");
        sensorData2.setTimestamp(LocalDateTime.now());
        sensorDataList.add(sensorData2);

        // 设置Mock行为（第二条数据插入失败）
        when(mysqlJdbcTemplate.batchUpdate(anyString(), any(List.class)))
                .thenThrow(new RuntimeException("Invalid sensor_id"));

        // 执行批量插入并验证异常
        assertThrows(RuntimeException.class, () -> {
            mySQLMapper.batchInsertSensorData(sensorDataList);
        });
    }

    /**
     * 自定义RowMapper测试
     */
    @Test
    void testRobotRowMapper() throws SQLException {
        // 准备模拟数据
        when(resultSet.getString("robot_id")).thenReturn("R001");
        when(resultSet.getString("robot_name")).thenReturn("机器人1号");
        when(resultSet.getString("robot_type")).thenReturn("AGV");
        when(resultSet.getString("status")).thenReturn("ONLINE");
        when(resultSet.getString("location")).thenReturn("车间A");
        when(resultSet.getTimestamp("create_time")).thenReturn(new java.sql.Timestamp(System.currentTimeMillis()));
        when(resultSet.getTimestamp("update_time")).thenReturn(new java.sql.Timestamp(System.currentTimeMillis()));

        // 创建RowMapper并映射
        MySQLMapper.RobotRowMapper rowMapper = new MySQLMapper.RobotRowMapper();
        Robot robot = rowMapper.mapRow(resultSet, 1);

        // 验证结果
        assertNotNull(robot);
        assertEquals("R001", robot.getRobotId());
        assertEquals("机器人1号", robot.getRobotName());
        assertEquals("AGV", robot.getRobotType());
        assertEquals("ONLINE", robot.getStatus());
        assertEquals("车间A", robot.getLocation());
    }
}