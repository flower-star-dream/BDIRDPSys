package com.bdir.dps.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 监控指标配置类
 * 配置Micrometer指标收集
 *
 * @author BDIRDPSys开发团队
 * @since 2026-01-16
 */
@Configuration
public class MetricsConfig {

    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicInteger activeWebSocketConnections = new AtomicInteger(0);

    /**
     * 配置Prometheus注册表
     */
    @Bean
    public PrometheusMeterRegistry prometheusMeterRegistry() {
        return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }

    /**
     * 配置JVM内存指标
     */
    @Bean
    public JvmMemoryMetrics jvmMemoryMetrics() {
        return new JvmMemoryMetrics();
    }

    /**
     * 配置JVM GC指标
     */
    @Bean
    public JvmGcMetrics jvmGcMetrics() {
        return new JvmGcMetrics();
    }

    /**
     * 配置处理器指标
     */
    @Bean
    public ProcessorMetrics processorMetrics() {
        return new ProcessorMetrics();
    }

    /**
     * 配置运行时间指标
     */
    @Bean
    public UptimeMetrics uptimeMetrics() {
        return new UptimeMetrics();
    }

    /**
     * 配置自定义业务指标
     */
    @Bean
    public BusinessMetrics businessMetrics(MeterRegistry meterRegistry) {
        return new BusinessMetrics(meterRegistry);
    }

    /**
     * 业务指标类
     */
    public static class BusinessMetrics {
        private final MeterRegistry meterRegistry;
        private final Counter sensorDataReceivedCounter;
        private final Counter robotCommandCounter;
        private final Counter errorCounter;
        private final Timer queryTimer;
        private final Timer dataIngestionTimer;
        private final AtomicInteger activeConnections;
        private final AtomicInteger activeWebSocketConnections;

        public BusinessMetrics(MeterRegistry meterRegistry) {
            this.meterRegistry = meterRegistry;

            // 传感器数据接收计数器
            this.sensorDataReceivedCounter = Counter.builder("sensor_data_received_total")
                    .description("Total number of sensor data received")
                    .register(meterRegistry);

            // 机器人命令计数器
            this.robotCommandCounter = Counter.builder("robot_command_total")
                    .description("Total number of robot commands sent")
                    .tag("type", "command")
                    .register(meterRegistry);

            // 错误计数器
            this.errorCounter = Counter.builder("application_errors_total")
                    .description("Total number of application errors")
                    .tag("layer", "application")
                    .register(meterRegistry);

            // 查询耗时计时器
            this.queryTimer = Timer.builder("database_query_duration_seconds")
                    .description("Database query duration in seconds")
                    .register(meterRegistry);

            // 数据摄取耗时计时器
            this.dataIngestionTimer = Timer.builder("data_ingestion_duration_seconds")
                    .description("Data ingestion duration in seconds")
                    .register(meterRegistry);

            // 活跃连接数
            this.activeConnections = new AtomicInteger(0);
            Gauge.builder("active_connections")
                    .description("Number of active database connections")
                    .register(meterRegistry, this, BusinessMetrics::getActiveConnections);

            // WebSocket活跃连接数
            this.activeWebSocketConnections = new AtomicInteger(0);
            Gauge.builder("websocket_connections_active")
                    .description("Number of active WebSocket connections")
                    .register(meterRegistry, this, BusinessMetrics::getActiveWebSocketConnections);
        }

        /**
         * 记录传感器数据接收
         */
        public void recordSensorDataReceived(String sensorType) {
            sensorDataReceivedCounter.increment("sensor_type", sensorType);
        }

        /**
         * 记录机器人命令
         */
        public void recordRobotCommand(String commandType) {
            robotCommandCounter.increment("command_type", commandType);
        }

        /**
         * 记录错误
         */
        public void recordError(String errorType) {
            errorCounter.increment("error_type", errorType);
        }

        /**
         * 记录查询耗时
         */
        public void recordQueryDuration(double durationSeconds, String queryType, String status) {
            queryTimer.record(durationSeconds, "query_type", queryType, "status", status);
        }

        /**
         * 记录数据摄取耗时
         */
        public void recordDataIngestionDuration(double durationSeconds, String dataType, String status) {
            dataIngestionTimer.record(durationSeconds, "data_type", dataType, "status", status);
        }

        /**
         * 增加活跃连接数
         */
        public void incrementActiveConnections() {
            activeConnections.incrementAndGet();
        }

        /**
         * 减少活跃连接数
         */
        public void decrementActiveConnections() {
            activeConnections.decrementAndGet();
        }

        /**
         * 增加WebSocket活跃连接数
         */
        public void incrementActiveWebSocketConnections() {
            activeWebSocketConnections.incrementAndGet();
        }

        /**
         * 减少WebSocket活跃连接数
         */
        public void decrementActiveWebSocketConnections() {
            activeWebSocketConnections.decrementAndGet();
        }

        public int getActiveConnections() {
            return activeConnections.get();
        }

        public int getActiveWebSocketConnections() {
            return activeWebSocketConnections.get();
        }
    }
}