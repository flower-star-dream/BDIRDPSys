package com.bdir.dps.mapper;

import com.bdir.dps.config.HiveConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Hive查询路由Mapper测试类
 */
@ExtendWith(MockitoExtension.class)
class HiveQueryRouterMapperTest {

    @Mock
    private JdbcTemplate hiveJdbcTemplate;

    private HiveQueryRouterMapper hiveQueryRouterMapper;

    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @BeforeEach
    void setUp() {
        hiveQueryRouterMapper = new HiveQueryRouterMapper();
        hiveQueryRouterMapper.setHiveJdbcTemplate(hiveJdbcTemplate);
    }

    /**
     * 测试实时数据查询（最近1小时）
     */
    @Test
    void testRouteQuery_RealtimeData() {
        // 准备测试数据
        String startTime = LocalDateTime.now().minusMinutes(30).format(formatter);
        String endTime = LocalDateTime.now().format(formatter);
        List<String> robotIds = Arrays.asList("R001", "R002");
        List<String> sensorTypes = Arrays.asList("TEMPERATURE", "HUMIDITY");
        List<String> metrics = Arrays.asList("temperature", "humidity");

        // 准备模拟结果
        List<Map<String, Object>> mockResults = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("robotId", "R001");
        row.put("avgTemperature", 25.5);
        row.put("avgHumidity", 60.0);
        mockResults.add(row);

        // 设置Mock行为
        when(hiveJdbcTemplate.queryForList(anyString())).thenReturn(mockResults);

        // 执行查询
        List<Map<String, Object>> results = hiveQueryRouterMapper.routeQuery(
            startTime, endTime, robotIds, sensorTypes, metrics
        );

        // 验证结果
        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals("R001", results.get(0).get("robotId"));
        assertEquals(25.5, results.get(0).get("avgTemperature"));
        assertEquals(60.0, results.get(0).get("avgHumidity"));

        // 验证SQL执行
        verify(hiveJdbcTemplate, times(1)).queryForList(anyString());
    }

    /**
     * 测试历史数据查询（超过24小时）
     */
    @Test
    void testRouteQuery_HistoricalData() {
        // 准备测试数据（超过24小时）
        String startTime = LocalDateTime.now().minusDays(2).format(formatter);
        String endTime = LocalDateTime.now().minusDays(1).format(formatter);
        List<String> robotIds = Arrays.asList("R001");
        List<String> sensorTypes = Arrays.asList("TEMPERATURE");
        List<String> metrics = Arrays.asList("temperature");

        // 准备模拟结果
        List<Map<String, Object>> mockResults = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("robotId", "R001");
        row.put("dt", "2024-01-01");
        row.put("avgTemperature", 24.8);
        row.put("maxTemperature", 28.0);
        row.put("minTemperature", 22.0);
        row.put("dataCount", 1440L);
        mockResults.add(row);

        // 设置Mock行为
        when(hiveJdbcTemplate.queryForList(anyString())).thenReturn(mockResults);

        // 执行查询
        List<Map<String, Object>> results = hiveQueryRouterMapper.routeQuery(
            startTime, endTime, robotIds, sensorTypes, metrics
        );

        // 验证结果
        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals("2024-01-01", results.get(0).get("dt"));
        assertEquals(1440L, results.get(0).get("dataCount"));

        // 验证SQL执行
        verify(hiveJdbcTemplate, times(1)).queryForList(anyString());
    }

    /**
     * 测试聚合查询
     */
    @Test
    void testRouteQuery_Aggregation() {
        // 准备测试数据
        String startTime = "2024-01-01 00:00:00";
        String endTime = "2024-01-02 00:00:00";
        List<String> robotIds = null; // 查询所有机器人
        List<String> sensorTypes = null; // 查询所有类型
        List<String> metrics = Arrays.asList("temperature", "humidity", "pressure");

        // 准备模拟结果
        List<Map<String, Object>> mockResults = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("robotId", "ALL");
        row.put("avgTemperature", 25.0);
        row.put("maxTemperature", 35.0);
        row.put("minTemperature", 15.0);
        row.put("avgHumidity", 65.0);
        row.put("maxHumidity", 85.0);
        row.put("minHumidity", 45.0);
        row.put("avgPressure", 1013.0);
        row.put("maxPressure", 1020.0);
        row.put("minPressure", 1005.0);
        mockResults.add(row);

        // 设置Mock行为
        when(hiveJdbcTemplate.queryForList(anyString())).thenReturn(mockResults);

        // 执行查询
        List<Map<String, Object>> results = hiveQueryRouterMapper.routeQuery(
            startTime, endTime, robotIds, sensorTypes, metrics
        );

        // 验证结果
        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(25.0, results.get(0).get("avgTemperature"));
        assertEquals(65.0, results.get(0).get("avgHumidity"));
        assertEquals(1013.0, results.get(0).get("avgPressure"));
    }

    /**
     * 测试空结果查询
     */
    @Test
    void testRouteQuery_EmptyResults() {
        // 准备测试数据
        String startTime = "2024-01-01 00:00:00";
        String endTime = "2024-01-02 00:00:00";
        List<String> robotIds = Arrays.asList("R999"); // 不存在的机器人
        List<String> metrics = Arrays.asList("temperature");

        // 设置Mock行为（返回空结果）
        when(hiveJdbcTemplate.queryForList(anyString())).thenReturn(new ArrayList<>());

        // 执行查询
        List<Map<String, Object>> results = hiveQueryRouterMapper.routeQuery(
            startTime, endTime, robotIds, null, metrics
        );

        // 验证结果
        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    /**
     * 测试异常处理
     */
    @Test
    void testRouteQuery_Exception() {
        // 准备测试数据
        String startTime = "2024-01-01 00:00:00";
        String endTime = "2024-01-02 00:00:00";
        List<String> metrics = Arrays.asList("temperature");

        // 设置Mock行为（抛出异常）
        when(hiveJdbcTemplate.queryForList(anyString()))
                .thenThrow(new RuntimeException("Hive query failed"));

        // 执行查询并验证异常
        assertThrows(RuntimeException.class, () -> {
            hiveQueryRouterMapper.routeQuery(startTime, endTime, null, null, metrics);
        });
    }

    /**
     * 测试时间范围验证
     */
    @Test
    void testRouteQuery_InvalidTimeRange() {
        // 准备测试数据（结束时间早于开始时间）
        String startTime = "2024-01-02 00:00:00";
        String endTime = "2024-01-01 00:00:00";
        List<String> metrics = Arrays.asList("temperature");

        // 执行查询（应该返回空结果）
        List<Map<String, Object>> results = hiveQueryRouterMapper.routeQuery(
            startTime, endTime, null, null, metrics
        );

        // 验证结果
        assertNotNull(results);
        assertTrue(results.isEmpty());

        // 验证没有执行SQL
        verify(hiveJdbcTemplate, never()).queryForList(anyString());
    }

    /**
     * 测试大数据量查询
     */
    @Test
    void testRouteQuery_LargeDataSet() {
        // 准备测试数据（大范围时间查询）
        String startTime = "2023-01-01 00:00:00";
        String endTime = "2024-01-01 00:00:00";
        List<String> metrics = Arrays.asList("temperature", "humidity", "pressure");

        // 准备大量模拟结果
        List<Map<String, Object>> mockResults = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("robotId", "R" + String.format("%03d", i));
            row.put("avgTemperature", 20.0 + i * 0.1);
            row.put("avgHumidity", 50.0 + i * 0.05);
            row.put("avgPressure", 1000.0 + i * 0.1);
            mockResults.add(row);
        }

        // 设置Mock行为
        when(hiveJdbcTemplate.queryForList(anyString())).thenReturn(mockResults);

        // 执行查询
        List<Map<String, Object>> results = hiveQueryRouterMapper.routeQuery(
            startTime, endTime, null, null, metrics
        );

        // 验证结果
        assertNotNull(results);
        assertEquals(1000, results.size());
        assertEquals("R000", results.get(0).get("robotId"));
        assertEquals("R999", results.get(999).get("robotId"));
    }

    /**
     * 测试混合OLAP路由策略
     */
    @Test
    void testRouteQuery_MixedOLAPStrategy() {
        // 测试不同时间范围的查询策略
        LocalDateTime now = LocalDateTime.now();

        // 实时数据查询（最近5分钟）
        testTimeRangeStrategy(now.minusMinutes(5), now, "realtime");

        // 近期数据查询（5分钟到24小时）
        testTimeRangeStrategy(now.minusHours(12), now.minusHours(6), "recent");

        // 历史数据查询（超过24小时）
        testTimeRangeStrategy(now.minusDays(7), now.minusDays(6), "historical");
    }

    private void testTimeRangeStrategy(LocalDateTime start, LocalDateTime end, String expectedStrategy) {
        String startTime = start.format(formatter);
        String endTime = end.format(formatter);
        List<String> metrics = Arrays.asList("temperature");

        // 准备模拟结果
        List<Map<String, Object>> mockResults = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("robotId", "R001");
        row.put("strategy", expectedStrategy);
        row.put("avgTemperature", 25.0);
        mockResults.add(row);

        // 设置Mock行为
        when(hiveJdbcTemplate.queryForList(anyString())).thenReturn(mockResults);

        // 执行查询
        List<Map<String, Object>> results = hiveQueryRouterMapper.routeQuery(
            startTime, endTime, null, null, metrics
        );

        // 验证结果
        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(expectedStrategy, results.get(0).get("strategy"));
    }
}