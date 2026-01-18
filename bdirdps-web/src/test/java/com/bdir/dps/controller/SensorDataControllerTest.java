package com.bdir.dps.controller;

import com.bdir.dps.common.Result;
import com.bdir.dps.entity.SensorData;
import com.bdir.dps.mapper.HiveQueryRouterMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * 传感器数据控制器测试类
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class SensorDataControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @MockBean
    private HiveQueryRouterMapper hiveQueryRouterMapper;

    private SensorData testSensorData;

    @BeforeEach
    void setUp() {
        testSensorData = new SensorData();
        testSensorData.setDataId("test-123");
        testSensorData.setRobotId("R001");
        testSensorData.setSensorId("S001");
        testSensorData.setSensorType("TEMPERATURE");
        testSensorData.setTimestamp(LocalDateTime.now());

        Map<String, Double> metrics = new HashMap<>();
        metrics.put("temperature", 25.5);
        metrics.put("humidity", 60.0);
        testSensorData.setMetrics(metrics);
    }

    /**
     * 测试接收传感器数据
     */
    @Test
    void testCollectSensorData() throws Exception {
        // 准备请求数据
        String requestBody = objectMapper.writeValueAsString(testSensorData);

        // 执行请求
        mockMvc.perform(post("/api/v1/sensor-data/clect")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("数据接收成功"))
                .andExpect(jsonPath("$.dataId").value("test-123"));

        // 验证Kafka发送
        verify(kafkaTemplate, times(1)).send(
            eq("sensor-data-temperature"),
            eq("R001#S001"),
            anyString()
        );
    }

    /**
     * 测试批量接收传感器数据
     */
    @Test
    void testCollectBatchSensorData() throws Exception {
        // 准备批量数据
        List<SensorData> sensorDataList = Arrays.asList(testSensorData, testSensorData);
        String requestBody = objectMapper.writeValueAsString(sensorDataList);

        // 执行请求
        mockMvc.perform(post("/api/v1/sensor-data/clect/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.successCount").value(2))
                .andExpect(jsonPath("$.failCount").value(0));

        // 验证Kafka发送次数
        verify(kafkaTemplate, times(2)).send(
            eq("sensor-data-temperature"),
            eq("R001#S001"),
            anyString()
        );
    }

    /**
     * 测试查询传感器数据
     */
    @Test
    void testQuerySensorData() throws Exception {
        // 准备模拟数据
        List<Map<String, Object>> mockData = new ArrayList<>();
        Map<String, Object> data = new HashMap<>();
        data.put("robotId", "R001");
        data.put("avgTemperature", 25.5);
        data.put("avgHumidity", 60.0);
        mockData.add(data);

        // 设置Mock行为
        when(hiveQueryRouterMapper.routeQuery(anyString(), anyString(), any(), any(), any()))
                .thenReturn(mockData);

        // 执行请求
        mockMvc.perform(get("/api/v1/sensor-data/query")
                .param("startTime", "2024-01-01 00:00:00")
                .param("endTime", "2024-01-02 00:00:00")
                .param("robotIds", "R001")
                .param("sensorTypes", "TEMPERATURE")
                .param("metrics", "temperature,humidity")
                .param("page", "1")
                .param("size", "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.page").value(1))
                .andExpect(jsonPath("$.size").value(20));

        // 验证查询调用
        verify(hiveQueryRouterMapper, times(1)).routeQuery(
            eq("2024-01-01 00:00:00"),
            eq("2024-01-02 00:00:00"),
            any(),
            any(),
            any()
        );
    }

    /**
     * 测试获取实时传感器数据
     */
    @Test
    void testGetRealtimeData() throws Exception {
        // 准备模拟数据
        List<Map<String, Object>> mockData = new ArrayList<>();
        Map<String, Object> data = new HashMap<>();
        data.put("robotId", "R001");
        data.put("avgTemperature", 25.5);
        data.put("avgHumidity", 60.0);
        data.put("avgPressure", 1013.25);
        mockData.add(data);

        // 设置Mock行为
        when(hiveQueryRouterMapper.routeQuery(anyString(), anyString(), any(), any(), any()))
                .thenReturn(mockData);

        // 执行请求
        mockMvc.perform(get("/api/v1/sensor-data/realtime")
                .param("robotIds", "R001")
                .param("sensorTypes", "TEMPERATURE")
                .param("limit", "50"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.count").value(1))
                .andExpect(jsonPath("$.timestamp").exists());
    }

    /**
     * 测试获取传感器数据统计
     */
    @Test
    void testGetStatistics() throws Exception {
        // 准备模拟数据
        List<Map<String, Object>> mockData = new ArrayList<>();
        Map<String, Object> data = new HashMap<>();
        data.put("robotId", "R001");
        data.put("avgTemperature", 25.5);
        data.put("maxTemperature", 30.0);
        data.put("minTemperature", 20.0);
        data.put("avgHumidity", 60.0);
        data.put("maxHumidity", 70.0);
        data.put("minHumidity", 50.0);
        mockData.add(data);

        // 设置Mock行为
        when(hiveQueryRouterMapper.routeQuery(anyString(), anyString(), any(), any(), any()))
                .thenReturn(mockData);

        // 执行请求
        mockMvc.perform(get("/api/v1/sensor-data/statistics")
                .param("startTime", "2024-01-01 00:00:00")
                .param("endTime", "2024-01-02 00:00:00")
                .param("robotIds", "R001")
                .param("groupBy", "robot"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.statistics").exists())
                .andExpect(jsonPath("$.statistics.totalRecords").value(1))
                .andExpect(jsonPath("$.statistics.temperature").exists());
    }

    /**
     * 测试异常检测
     */
    @Test
    void testDetectAnomalies() throws Exception {
        // 准备请求数据
        Map<String, Object> request = new HashMap<>();
        request.put("robotIds", Arrays.asList("R001"));
        request.put("startTime", "2024-01-01 00:00:00");
        request.put("endTime", "2024-01-02 00:00:00");
        request.put("algorithms", Arrays.asList("statistical", "threshold"));

        String requestBody = objectMapper.writeValueAsString(request);

        // 准备模拟数据
        List<Map<String, Object>> mockData = new ArrayList<>();
        Map<String, Object> data = new HashMap<>();
        data.put("robotId", "R001");
        data.put("avgTemperature", 150.0); // 异常值
        mockData.add(data);

        // 设置Mock行为
        when(hiveQueryRouterMapper.routeQuery(anyString(), anyString(), any(), any(), any()))
                .thenReturn(mockData);

        // 执行请求
        mockMvc.perform(post("/api/v1/sensor-data/anomaly-detection")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.anomalies").isArray())
                .andExpect(jsonPath("$.algorithmUsed").isArray())
                .andExpect(jsonPath("$.totalChecked").value(1));
    }

    /**
     * 测试数据导出
     */
    @Test
    void testExportData() throws Exception {
        // 准备模拟数据
        List<Map<String, Object>> mockData = new ArrayList<>();
        Map<String, Object> data = new HashMap<>();
        data.put("robotId", "R001");
        data.put("robotName", "机器人1号");
        data.put("robotType", "AGV");
        data.put("avgTemperature", 25.5);
        data.put("maxTemperature", 30.0);
        data.put("minTemperature", 20.0);
        data.put("avgHumidity", 60.0);
        data.put("maxHumidity", 70.0);
        data.put("minHumidity", 50.0);
        data.put("avgPressure", 1013.25);
        data.put("dataCount", 100);
        mockData.add(data);

        // 设置Mock行为
        when(hiveQueryRouterMapper.routeQuery(anyString(), anyString(), any(), any(), any()))
                .thenReturn(mockData);

        // 测试CSV导出
        mockMvc.perform(get("/api/v1/sensor-data/export")
                .param("startTime", "2024-01-01 00:00:00")
                .param("endTime", "2024-01-02 00:00:00")
                .param("format", "csv"))
                .andExpect(status().isOk())
                .andExpect(header().string("Content-Type", "text/csv"))
                .andExpect(header().string("Content-Disposition", containsString(".csv")));

        // 测试JSON导出
        mockMvc.perform(get("/api/v1/sensor-data/export")
                .param("startTime", "2024-01-01 00:00:00")
                .param("endTime", "2024-01-02 00:00:00")
                .param("format", "json"))
                .andExpect(status().isOk())
                .andExpect(header().string("Content-Type", "application/json"))
                .andExpect(header().string("Content-Disposition", containsString(".json")));
    }

    /**
     * 测试参数验证失败
     */
    @Test
    void testInvalidParameter() throws Exception {
        // 创建无效数据（缺少必填字段）
        SensorData invalidData = new SensorData();
        invalidData.setDataId("test-123");
        // robotId为空

        String requestBody = objectMapper.writeValueAsString(invalidData);

        // 执行请求
        mockMvc.perform(post("/api/v1/sensor-data/clect")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("数据验证失败"));

        // 验证Kafka未发送
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    /**
     * 测试获取传感器类型列表
     */
    @Test
    void testGetSensorTypes() throws Exception {
        mockMvc.perform(get("/api/v1/sensor-data/sensor-types"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.sensorTypes").isArray())
                .andExpect(jsonPath("$.sensorTypes.length()").value(6));
    }

    /**
     * 测试获取异常检测算法列表
     */
    @Test
    void testGetAnomalyAlgorithms() throws Exception {
        mockMvc.perform(get("/api/v1/sensor-data/anomaly-algorithms"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.algorithms").isArray())
                .andExpect(jsonPath("$.algorithms.length()").value(4));
    }

    /**
     * 测试数据质量报告
     */
    @Test
    void testGetDataQualityReport() throws Exception {
        // 准备模拟数据
        List<Map<String, Object>> mockData = new ArrayList<>();
        Map<String, Object> data = new HashMap<>();
        data.put("avgTemperature", 25.5);
        data.put("avgHumidity", 60.0);
        data.put("avgPressure", 1013.25);
        data.put("data_count", 100);
        mockData.add(data);

        // 设置Mock行为
        when(hiveQueryRouterMapper.routeQuery(anyString(), anyString(), any(), any(), anyList()))
                .thenReturn(mockData);

        // 执行请求
        mockMvc.perform(get("/api/v1/sensor-data/quality-report")
                .param("startTime", "2024-01-01 00:00:00")
                .param("endTime", "2024-01-02 00:00:00"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.qualityReport").exists())
                .andExpect(jsonPath("$.qualityReport.totalRecords").value(1))
                .andExpect(jsonPath("$.qualityReport.completenessRate").exists());
    }
}