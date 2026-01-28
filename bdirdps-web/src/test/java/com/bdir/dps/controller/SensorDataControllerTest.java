package com.bdir.dps.controller;

import com.bdir.dps.entity.SensorData;
import com.bdir.dps.mapper.HiveQueryRouterMapper;
import com.bdir.dps.service.SensorDataService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;

import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * 传感器数据控制器测试类
 *
 * @author BDIRDPSys开发团队
 * @since 2026-01-16
 */
@WebMvcTest(SensorDataController.class)
class SensorDataControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private SensorDataService sensorDataService;

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
        testSensorData.setTimestamp(System.currentTimeMillis());

        Map<String, Object> metrics = new HashMap<>();
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
        mockMvc.perform(post("/api/v1/sensor-data/collect")
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
        mockMvc.perform(post("/api/v1/sensor-data/collect/batch")
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
                .andExpect(jsonPath("$.data[0].robotId").value("R001"))
                .andExpect(jsonPath("$.data[0].avgTemperature").value(25.5));

        // 验证查询调用
        verify(hiveQueryRouterMapper, times(1)).routeQuery(
                eq("TEMPERATURE"),
                eq("temperature,humidity"),
                anyList(),
                any(Date.class),
                any(Date.class)
        );
    }

    /**
     * 测试查询统计信息
     */
    @Test
    void testGetStatistics() throws Exception {
        // 准备模拟数据
        List<Map<String, Object>> mockData = new ArrayList<>();
        Map<String, Object> data = new HashMap<>();
        data.put("robotId", "R001");
        data.put("sensorType", "TEMPERATURE");
        data.put("avgValue", 25.5);
        data.put("maxValue", 30.0);
        data.put("minValue", 20.0);
        mockData.add(data);

        // 设置Mock行为
        when(hiveQueryRouterMapper.routeQuery(anyString(), anyString(), anyList(), any(), any()))
                .thenReturn(mockData);

        // 执行请求
        mockMvc.perform(get("/api/v1/sensor-data/statistics")
                .param("startTime", "2024-01-01 00:00:00")
                .param("endTime", "2024-01-02 00:00:00")
                .param("robotIds", "R001")
                .param("sensorTypes", "TEMPERATURE")
                .param("metrics", "temperature"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data[0].robotId").value("R001"))
                .andExpect(jsonPath("$.data[0].avgValue").value(25.5))
                .andExpect(jsonPath("$.data[0].maxValue").value(30.0))
                .andExpect(jsonPath("$.data[0].minValue").value(20.0));
    }

    /**
     * 测试异常处理 - 无效参数
     */
    @Test
    void testInvalidParameter() throws Exception {
        mockMvc.perform(get("/api/v1/sensor-data/query")
                .param("startTime", "invalid-date")
                .param("endTime", "2024-01-02 00:00:00"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("参数错误"));
    }
}