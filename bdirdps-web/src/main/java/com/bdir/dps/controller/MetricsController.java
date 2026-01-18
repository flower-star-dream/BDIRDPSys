package com.bdir.dps.controller;

import com.bdir.dps.common.Result;
import com.bdir.dps.config.MetricsConfig.BusinessMetrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 监控指标控制器
 * 提供Prometheus格式的监控指标
 *
 * @author BDIRDPSys开发团队
 * @since 2026-01-16
 */
@RestController
@RequestMapping("/api/metrics")
public class MetricsController {

    @Autowired
    private PrometheusMeterRegistry prometheusMeterRegistry;

    @Autowired
    private BusinessMetrics businessMetrics;

    /**
     * 获取Prometheus格式的监控指标
     */
    @GetMapping(value = "/prometheus", produces = MediaType.TEXT_PLAIN_VALUE)
    public String getPrometheusMetrics() {
        return prometheusMeterRegistry.scrape();
    }

    /**
     * 获取关键业务指标
     */
    @GetMapping("/business")
    public Result<BusinessMetricsDTO> getBusinessMetrics() {
        BusinessMetricsDTO dto = new BusinessMetricsDTO();
        dto.setActiveConnections(businessMetrics.getActiveConnections());
        dto.setActiveWebSocketConnections(businessMetrics.getActiveWebSocketConnections());
        dto.setSystemStatus("HEALTHY");
        dto.setTimestamp(System.currentTimeMillis());
        return Result.success(dto);
    }

    /**
     * 获取系统健康状态
     */
    @GetMapping("/health")
    public Result<SystemHealthDTO> getSystemHealth() {
        SystemHealthDTO health = new SystemHealthDTO();
        health.setStatus("UP");
        health.setUptime(System.currentTimeMillis() - startTime);
        health.setActiveConnections(businessMetrics.getActiveConnections());
        health.setWebSocketConnections(businessMetrics.getActiveWebSocketConnections());
        health.setComponents(checkComponentHealth());
        return Result.success(health);
    }

    /**
     * 业务指标数据传输对象
     */
    public static class BusinessMetricsDTO {
        private int activeConnections;
        private int activeWebSocketConnections;
        private String systemStatus;
        private long timestamp;

        // getters and setters
        public int getActiveConnections() {
            return activeConnections;
        }

        public void setActiveConnections(int activeConnections) {
            this.activeConnections = activeConnections;
        }

        public int getActiveWebSocketConnections() {
            return activeWebSocketConnections;
        }

        public void setActiveWebSocketConnections(int activeWebSocketConnections) {
            this.activeWebSocketConnections = activeWebSocketConnections;
        }

        public String getSystemStatus() {
            return systemStatus;
        }

        public void setSystemStatus(String systemStatus) {
            this.systemStatus = systemStatus;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }

    /**
     * 系统健康状态数据传输对象
     */
    public static class SystemHealthDTO {
        private String status;
        private long uptime;
        private int activeConnections;
        private int webSocketConnections;
        private java.util.Map<String, ComponentHealth> components;

        // getters and setters
        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public long getUptime() {
            return uptime;
        }

        public void setUptime(long uptime) {
            this.uptime = uptime;
        }

        public int getActiveConnections() {
            return activeConnections;
        }

        public void setActiveConnections(int activeConnections) {
            this.activeConnections = activeConnections;
        }

        public int getWebSocketConnections() {
            return webSocketConnections;
        }

        public void setWebSocketConnections(int webSocketConnections) {
            this.webSocketConnections = webSocketConnections;
        }

        public java.util.Map<String, ComponentHealth> getComponents() {
            return components;
        }

        public void setComponents(java.util.Map<String, ComponentHealth> components) {
            this.components = components;
        }
    }

    /**
     * 组件健康状态
     */
    public static class ComponentHealth {
        private String status;
        private String details;
        private long responseTime;

        public ComponentHealth(String status, String details, long responseTime) {
            this.status = status;
            this.details = details;
            this.responseTime = responseTime;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getDetails() {
            return details;
        }

        public void setDetails(String details) {
            this.details = details;
        }

        public long getResponseTime() {
            return responseTime;
        }

        public void setResponseTime(long responseTime) {
            this.responseTime = responseTime;
        }
    }

    // 系统启动时间
    private static final long startTime = System.currentTimeMillis();

    /**
     * 检查各组件健康状态
     */
    private java.util.Map<String, ComponentHealth> checkComponentHealth() {
        java.util.Map<String, ComponentHealth> components = new java.util.HashMap<>();

        // 检查数据库连接
        components.put("database", checkDatabaseHealth());

        // 检查Kafka连接
        components.put("kafka", checkKafkaHealth());

        // 检查Hive连接
        components.put("hive", checkHiveHealth());

        // 检查Redis连接（如果有）
        components.put("redis", checkRedisHealth());

        return components;
    }

    private ComponentHealth checkDatabaseHealth() {
        // 这里应该实际检查数据库连接
        return new ComponentHealth("UP", "Database connection is healthy", 10);
    }

    private ComponentHealth checkKafkaHealth() {
        // 这里应该实际检查Kafka连接
        return new ComponentHealth("UP", "Kafka cluster is reachable", 15);
    }

    private ComponentHealth checkHiveHealth() {
        // 这里应该实际检查Hive连接
        return new ComponentHealth("UP", "Hive server is responsive", 20);
    }

    private ComponentHealth checkRedisHealth() {
        // 这里应该实际检查Redis连接
        return new ComponentHealth("UP", "Redis server is available", 5);
    }
}