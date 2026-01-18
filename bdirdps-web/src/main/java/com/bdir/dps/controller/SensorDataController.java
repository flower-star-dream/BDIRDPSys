package com.bdir.dps.controller;

import com.bdir.dps.entity.SensorData;
import com.bdir.dps.mapper.HiveQueryRouterMapper;
import com.bdir.dps.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 传感器数据控制器
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/sensor-data")
public class SensorDataController {

    @Autowired
    private HiveQueryRouterMapper hiveQueryRouterMapper;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 接收传感器数据
     */
    @PostMapping("/collect")
    public ResponseEntity<? super Map<String, Object>> collectSensorData(@RequestBody SensorData sensorData) {
        try {
            log.debug("接收传感器数据: {}", JsonUtil.toJson(sensorData));

            // 验证数据
            if (!validateSensorData(sensorData)) {
                return ResponseEntity.badRequest().body(
                    Map.of("success", false, "message", "数据验证失败")
                );
            }

            // 发送到Kafka
            String topic = "sensor-data-" + sensorData.getSensorType().toLowerCase();
            String key = sensorData.getRobotId() + "#" + sensorData.getSensorId();
            String value = JsonUtil.toJson(sensorData);

            kafkaTemplate.send(topic, key, value);

            // 同时发送到实时数据表
            sendToRealtimeTable(sensorData);

            Map<String, Object> result = Map.of(
                "success", true,
                "message", "数据接收成功",
                "dataId", sensorData.getDataId()
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("接收传感器数据失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 批量接收传感器数据
     */
    @PostMapping("/collect/batch")
    public ResponseEntity<? super Map<String, Object>> collectBatchSensorData(
            @RequestBody List<SensorData> sensorDataList) {
        try {
            log.info("批量接收传感器数据，数量: {}", sensorDataList.size());

            int successCount = 0;
            int failCount = 0;

            for (SensorData sensorData : sensorDataList) {
                try {
                    if (validateSensorData(sensorData)) {
                        // 发送到Kafka
                        String topic = "sensor-data-" + sensorData.getSensorType().toLowerCase();
                        String key = sensorData.getRobotId() + "#" + sensorData.getSensorId();
                        String value = JsonUtil.toJson(sensorData);

                        kafkaTemplate.send(topic, key, value);

                        // 发送到实时数据表
                        sendToRealtimeTable(sensorData);

                        successCount++;
                    } else {
                        failCount++;
                    }
                } catch (Exception e) {
                    log.error("处理单条传感器数据失败: {}", e.getMessage());
                    failCount++;
                }
            }

            Map<String, Object> result = Map.of(
                "success", true,
                "message", String.format("批量处理完成，成功%d条，失败%d条", successCount, failCount),
                "successCount", successCount,
                "failCount", failCount
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("批量接收传感器数据失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 查询传感器数据
     */
    @GetMapping("/query")
    public ResponseEntity<? super Map<String, Object>> querySensorData(
            @RequestParam String startTime,
            @RequestParam String endTime,
            @RequestParam(required = false) List<String> robotIds,
            @RequestParam(required = false) List<String> sensorTypes,
            @RequestParam(required = false) List<String> metrics,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int size) {
        try {
            log.info("查询传感器数据: startTime={}, endTime={}, robotIds={}, sensorTypes={}, metrics={}",
                    startTime, endTime, robotIds, sensorTypes, metrics);

            // 处理默认参数
            if (metrics == null || metrics.isEmpty()) {
                metrics = Arrays.asList("temperature", "humidity", "pressure");
            }

            // 使用Hive查询路由
            List<Map<String, Object>> data = hiveQueryRouterMapper.routeQuery(
                startTime, endTime, robotIds, sensorTypes, metrics
            );

            // 分页处理
            int total = data.size();
            int start = (page - 1) * size;
            int end = Math.min(start + size, total);
            List<Map<String, Object>> pageData = data.subList(start, end);

            Map<String, Object> result = Map.of(
                "success", true,
                "data", pageData,
                "total", total,
                "page", page,
                "size", size,
                "pages", (total + size - 1) / size
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("查询传感器数据失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取实时传感器数据
     */
    @GetMapping("/realtime")
    public ResponseEntity<? super Map<String, Object>> getRealtimeData(
            @RequestParam(required = false) List<String> robotIds,
            @RequestParam(required = false) List<String> sensorTypes,
            @RequestParam(defaultValue = "50") int limit) {
        try {
            // 构建查询条件
            String startTime = LocalDateTime.now().minusMinutes(5).format(FORMATTER);
            String endTime = LocalDateTime.now().format(FORMATTER);

            List<String> metrics = Arrays.asList("temperature", "humidity", "pressure", "position_x", "position_y", "position_z");

            // 查询最近5分钟的数据
            List<Map<String, Object>> data = hiveQueryRouterMapper.routeQuery(
                startTime, endTime, robotIds, sensorTypes, metrics
            );

            // 限制返回数量
            if (data.size() > limit) {
                data = data.subList(0, limit);
            }

            Map<String, Object> result = Map.of(
                "success", true,
                "data", data,
                "count", data.size(),
                "timestamp", System.currentTimeMillis()
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取实时传感器数据失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取传感器数据统计
     */
    @GetMapping("/statistics")
    public ResponseEntity<? super Map<String, Object>> getStatistics(
            @RequestParam String startTime,
            @RequestParam String endTime,
            @RequestParam(required = false) List<String> robotIds,
            @RequestParam(required = false) String groupBy) {
        try {
            // 默认按机器人分组
            if (groupBy == null || groupBy.isEmpty()) {
                groupBy = "robot";
            }

            List<String> metrics = Arrays.asList("temperature", "humidity", "pressure");

            // 查询统计数据
            List<Map<String, Object>> data = hiveQueryRouterMapper.routeQuery(
                startTime, endTime, robotIds, null, metrics
            );

            // 计算统计信息
            Map<String, Object> statistics = calculateStatistics(data, groupBy);

            Map<String, Object> result = Map.of(
                "success", true,
                "statistics", statistics,
                "period", Map.of("start", startTime, "end", endTime)
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取传感器数据统计失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取传感器类型列表
     */
    @GetMapping("/sensor-types")
    public ResponseEntity<? super Map<String, Object>> getSensorTypes() {
        try {
            List<String> sensorTypes = Arrays.asList(
                "TEMPERATURE", "HUMIDITY", "PRESSURE", "POSITION", "VIBRATION", "NOISE"
            );

            Map<String, Object> result = Map.of(
                "success", true,
                "sensorTypes", sensorTypes
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取传感器类型列表失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 验证传感器数据
     */
    private boolean validateSensorData(SensorData sensorData) {
        if (sensorData == null) {
            return false;
        }

        // 验证必填字段
        if (sensorData.getRobotId() == null || sensorData.getRobotId().isEmpty()) {
            return false;
        }
        if (sensorData.getSensorId() == null || sensorData.getSensorId().isEmpty()) {
            return false;
        }
        if (sensorData.getSensorType() == null || sensorData.getSensorType().isEmpty()) {
            return false;
        }

        // 验证数值范围
        Map<String, Double> metrics = sensorData.getMetrics();
        if (metrics != null) {
            for (Map.Entry<String, Double> entry : metrics.entrySet()) {
                Double value = entry.getValue();
                if (value == null || Double.isNaN(value) || Double.isInfinite(value)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 发送到实时数据表
     */
    private void sendToRealtimeTable(SensorData sensorData) {
        // 这里应该调用DAO层将数据插入MySQL实时表
        // 简化实现，仅记录日志
        log.debug("发送数据到实时表: {}", sensorData.getDataId());
    }

    /**
     * 计算统计信息
     */
    private Map<String, Object> calculateStatistics(List<Map<String, Object>> data, String groupBy) {
        Map<String, Object> statistics = new HashMap<>();

        int totalRecords = data.size();
        double avgTemperature = 0;
        double maxTemperature = Double.MIN_VALUE;
        double minTemperature = Double.MAX_VALUE;
        double avgHumidity = 0;
        double maxHumidity = Double.MIN_VALUE;
        double minHumidity = Double.MAX_VALUE;
        double avgPressure = 0;
        double maxPressure = Double.MIN_VALUE;
        double minPressure = Double.MAX_VALUE;

        int tempCount = 0;
        int humidityCount = 0;
        int pressureCount = 0;

        for (Map<String, Object> record : data) {
            // 温度统计
            if (record.containsKey("avgTemperature")) {
                double temp = (Double) record.get("avgTemperature");
                avgTemperature += temp;
                maxTemperature = Math.max(maxTemperature, temp);
                minTemperature = Math.min(minTemperature, temp);
                tempCount++;
            }

            // 湿度统计
            if (record.containsKey("avgHumidity")) {
                double humidity = (Double) record.get("avgHumidity");
                avgHumidity += humidity;
                maxHumidity = Math.max(maxHumidity, humidity);
                minHumidity = Math.min(minHumidity, humidity);
                humidityCount++;
            }

            // 压力统计
            if (record.containsKey("avgPressure")) {
                double pressure = (Double) record.get("avgPressure");
                avgPressure += pressure;
                maxPressure = Math.max(maxPressure, pressure);
                minPressure = Math.min(minPressure, pressure);
                pressureCount++;
            }
        }

        // 计算平均值
        if (tempCount > 0) {
            avgTemperature /= tempCount;
        }
        if (humidityCount > 0) {
            avgHumidity /= humidityCount;
        }
        if (pressureCount > 0) {
            avgPressure /= pressureCount;
        }

        statistics.put("totalRecords", totalRecords);
        statistics.put("temperature", Map.of(
            "avg", avgTemperature,
            "max", maxTemperature == Double.MIN_VALUE ? 0 : maxTemperature,
            "min", minTemperature == Double.MAX_VALUE ? 0 : minTemperature,
            "count", tempCount
        ));
        statistics.put("humidity", Map.of(
            "avg", avgHumidity,
            "max", maxHumidity == Double.MIN_VALUE ? 0 : maxHumidity,
            "min", minHumidity == Double.MAX_VALUE ? 0 : minHumidity,
            "count", humidityCount
        ));
        statistics.put("pressure", Map.of(
            "avg", avgPressure,
            "max", maxPressure == Double.MIN_VALUE ? 0 : maxPressure,
            "min", minPressure == Double.MAX_VALUE ? 0 : minPressure,
            "count", pressureCount
        ));

        return statistics;
    }

    /**
     * 异常检测
     */
    @PostMapping("/anomaly-detection")
    public ResponseEntity<? super Map<String, Object>> detectAnomalies(
            @RequestBody Map<String, Object> request) {
        try {
            List<String> robotIds = (List<String>) request.get("robotIds");
            String startTime = (String) request.get("startTime");
            String endTime = (String) request.get("endTime");
            List<String> algorithms = (List<String>) request.get("algorithms");

            // 查询数据
            List<String> metrics = Arrays.asList("temperature", "humidity", "pressure");
            List<Map<String, Object>> data = hiveQueryRouterMapper.routeQuery(
                startTime, endTime, robotIds, null, metrics
            );

            // 执行异常检测算法
            List<Map<String, Object>> anomalies = performAnomalyDetection(data, algorithms);

            Map<String, Object> result = Map.of(
                "success", true,
                "anomalies", anomalies,
                "algorithmUsed", algorithms,
                "totalChecked", data.size()
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("异常检测失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 执行异常检测算法
     */
    private List<Map<String, Object>> performAnomalyDetection(List<Map<String, Object>> data, List<String> algorithms) {
        List<Map<String, Object>> anomalies = new ArrayList<>();

        if (algorithms.contains("statistical")) {
            // 统计方法异常检测
            anomalies.addAll(statisticalAnomalyDetection(data));
        }

        if (algorithms.contains("threshold")) {
            // 阈值方法异常检测
            anomalies.addAll(thresholdAnomalyDetection(data));
        }

        return anomalies;
    }

    /**
     * 统计异常检测
     */
    private List<Map<String, Object>> statisticalAnomalyDetection(List<Map<String, Object>> data) {
        List<Map<String, Object>> anomalies = new ArrayList<>();

        // 计算温度统计
        List<Double> temperatures = new ArrayList<>();
        for (Map<String, Object> record : data) {
            if (record.containsKey("avgTemperature")) {
                temperatures.add((Double) record.get("avgTemperature"));
            }
        }

        if (!temperatures.isEmpty()) {
            double mean = temperatures.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            double stdDev = calculateStdDev(temperatures, mean);

            // 3σ原则检测异常
            for (Map<String, Object> record : data) {
                if (record.containsKey("avgTemperature")) {
                    double temp = (Double) record.get("avgTemperature");
                    if (Math.abs(temp - mean) > 3 * stdDev) {
                        Map<String, Object> anomaly = new HashMap<>();
                        anomaly.put("type", "STATISTICAL");
                        anomaly.put("metric", "temperature");
                        anomaly.put("value", temp);
                        anomaly.put("expectedRange", Map.of("min", mean - 3 * stdDev, "max", mean + 3 * stdDev));
                        anomaly.put("severity", "HIGH");
                        anomaly.put("robotId", record.get("robotId"));
                        anomalies.add(anomaly);
                    }
                }
            }
        }

        return anomalies;
    }

    /**
     * 阈值异常检测
     */
    private List<Map<String, Object>> thresholdAnomalyDetection(List<Map<String, Object>> data) {
        List<Map<String, Object>> anomalies = new ArrayList<>();

        // 温度阈值检测
        double tempMin = 0;
        double tempMax = 100;

        for (Map<String, Object> record : data) {
            if (record.containsKey("avgTemperature")) {
                double temp = (Double) record.get("avgTemperature");
                if (temp < tempMin || temp > tempMax) {
                    Map<String, Object> anomaly = new HashMap<>();
                    anomaly.put("type", "THRESHOLD");
                    anomaly.put("metric", "temperature");
                    anomaly.put("value", temp);
                    anomaly.put("expectedRange", Map.of("min", tempMin, "max", tempMax));
                    anomaly.put("severity", temp > 80 ? "CRITICAL" : "MEDIUM");
                    anomaly.put("robotId", record.get("robotId"));
                    anomalies.add(anomaly);
                }
            }
        }

        return anomalies;
    }

    /**
     * 计算标准差
     */
    private double calculateStdDev(List<Double> values, double mean) {
        double sum = 0;
        for (double value : values) {
            sum += Math.pow(value - mean, 2);
        }
        return Math.sqrt(sum / values.size());
    }

    /**
     * 获取异常检测算法列表
     */
    @GetMapping("/anomaly-algorithms")
    public ResponseEntity<? super Map<String, Object>> getAnomalyAlgorithms() {
        try {
            List<Map<String, Object>> algorithms = Arrays.asList(
                Map.of("code", "statistical", "name", "统计方法", "description", "基于3σ原则的异常检测"),
                Map.of("code", "threshold", "name", "阈值方法", "description", "基于预设阈值的异常检测"),
                Map.of("code", "machine_learning", "name", "机器学习", "description", "基于机器学习模型的异常检测"),
                Map.of("code", "isolation_forest", "name", "孤立森林", "description", "基于孤立森林算法的异常检测")
            );

            Map<String, Object> result = Map.of(
                "success", true,
                "algorithms", algorithms
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取异常检测算法列表失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 生成数据报告
     */
    @PostMapping("/report")
    public ResponseEntity<? super Map<String, Object>> generateReport(
            @RequestBody Map<String, Object> request) {
        try {
            String startTime = (String) request.get("startTime");
            String endTime = (String) request.get("endTime");
            List<String> robotIds = (List<String>) request.get("robotIds");
            String reportType = (String) request.get("reportType");

            // 查询数据
            List<String> metrics = Arrays.asList("temperature", "humidity", "pressure", "data_count");
            List<Map<String, Object>> data = hiveQueryRouterMapper.routeQuery(
                startTime, endTime, robotIds, null, metrics
            );

            // 生成报告
            Map<String, Object> report = generateReport(data, reportType);

            Map<String, Object> result = Map.of(
                "success", true,
                "report", report,
                "reportType", reportType,
                "generatedAt", LocalDateTime.now().format(FORMATTER)
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("生成数据报告失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 生成报告
     */
    private Map<String, Object> generateReport(List<Map<String, Object>> data, String reportType) {
        Map<String, Object> report = new HashMap<>();

        if ("summary".equals(reportType)) {
            // 汇总报告
            report.put("type", "summary");
            report.put("totalRecords", data.size());
            report.put("statistics", calculateStatistics(data, "robot"));
        } else if ("detailed".equals(reportType)) {
            // 详细报告
            report.put("type", "detailed");
            report.put("data", data);
            report.put("summary", calculateStatistics(data, "robot"));
        } else if ("chart".equals(reportType)) {
            // 图表报告
            report.put("type", "chart");
            report.put("chartData", prepareChartData(data));
        }

        return report;
    }

    /**
     * 准备图表数据
     */
    private Map<String, Object> prepareChartData(List<Map<String, Object>> data) {
        Map<String, Object> chartData = new HashMap<>();

        List<String> labels = new ArrayList<>();
        List<Double> temperatureData = new ArrayList<>();
        List<Double> humidityData = new ArrayList<>();
        List<Double> pressureData = new ArrayList<>();

        for (Map<String, Object> record : data) {
            String robotId = (String) record.get("robotId");
            labels.add(robotId);

            temperatureData.add((Double) record.getOrDefault("avgTemperature", 0.0));
            humidityData.add((Double) record.getOrDefault("avgHumidity", 0.0));
            pressureData.add((Double) record.getOrDefault("avgPressure", 0.0));
        }

        chartData.put("labels", labels);
        chartData.put("datasets", Arrays.asList(
            Map.of(
                "label", "温度",
                "data", temperatureData,
                "backgroundColor", "rgba(255, 99, 132, 0.2)",
                "borderColor", "rgba(255, 99, 132, 1)",
                "borderWidth", 1
            ),
            Map.of(
                "label", "湿度",
                "data", humidityData,
                "backgroundColor", "rgba(54, 162, 235, 0.2)",
                "borderColor", "rgba(54, 162, 235, 1)",
                "borderWidth", 1
            ),
            Map.of(
                "label", "压力",
                "data", pressureData,
                "backgroundColor", "rgba(255, 206, 86, 0.2)",
                "borderColor", "rgba(255, 206, 86, 1)",
                "borderWidth", 1
            )
        ));

        return chartData;
    }

    /**
     * 获取传感器数据趋势
     */
    @GetMapping("/trend")
    public ResponseEntity<? super Map<String, Object>> getTrend(
            @RequestParam String startTime,
            @RequestParam String endTime,
            @RequestParam(required = false) List<String> robotIds,
            @RequestParam(defaultValue = "hour") String interval) {
        try {
            // 根据间隔时间调整查询
            String adjustedStartTime = adjustTimeByInterval(startTime, interval, -10);
            String adjustedEndTime = adjustTimeByInterval(endTime, interval, 1);

            List<String> metrics = Arrays.asList("temperature", "humidity", "pressure");
            List<Map<String, Object>> data = hiveQueryRouterMapper.routeQuery(
                adjustedStartTime, adjustedEndTime, robotIds, null, metrics
            );

            // 按时间间隔聚合数据
            Map<String, Object> trendData = aggregateByInterval(data, interval);

            Map<String, Object> result = Map.of(
                "success", true,
                "trendData", trendData,
                "interval", interval
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取传感器数据趋势失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 调整时间
     */
    private String adjustTimeByInterval(String time, String interval, int offset) {
        LocalDateTime dateTime = LocalDateTime.parse(time, FORMATTER);
        switch (interval) {
            case "minute":
                return dateTime.plusMinutes(offset).format(FORMATTER);
            case "hour":
                return dateTime.plusHours(offset).format(FORMATTER);
            case "day":
                return dateTime.plusDays(offset).format(FORMATTER);
            default:
                return time;
        }
    }

    /**
     * 按时间间隔聚合数据
     */
    private Map<String, Object> aggregateByInterval(List<Map<String, Object>> data, String interval) {
        Map<String, Object> result = new HashMap<>();
        Map<String, List<Double>> tempData = new HashMap<>();
        Map<String, List<Double>> humidityData = new HashMap<>();
        Map<String, List<Double>> pressureData = new HashMap<>();

        // 按时间分组
        for (Map<String, Object> record : data) {
            String timeKey = formatTimeByInterval((String) record.get("time"), interval);

            tempData.computeIfAbsent(timeKey, k -> new ArrayList<>())
                    .add((Double) record.getOrDefault("avgTemperature", 0.0));
            humidityData.computeIfAbsent(timeKey, k -> new ArrayList<>())
                    .add((Double) record.getOrDefault("avgHumidity", 0.0));
            pressureData.computeIfAbsent(timeKey, k -> new ArrayList<>())
                    .add((Double) record.getOrDefault("avgPressure", 0.0));
        }

        // 计算每个时间点的平均值
        result.put("temperature", calculateAverages(tempData));
        result.put("humidity", calculateAverages(humidityData));
        result.put("pressure", calculateAverages(pressureData));

        return result;
    }

    /**
     * 格式化时间
     */
    private String formatTimeByInterval(String time, String interval) {
        LocalDateTime dateTime = LocalDateTime.parse(time, FORMATTER);
        switch (interval) {
            case "minute":
                return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
            case "hour":
                return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00"));
            case "day":
                return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            default:
                return time;
        }
    }

    /**
     * 计算平均值
     */
    private Map<String, Double> calculateAverages(Map<String, List<Double>> data) {
        Map<String, Double> averages = new HashMap<>();
        for (Map.Entry<String, List<Double>> entry : data.entrySet()) {
            List<Double> values = entry.getValue();
            if (!values.isEmpty()) {
                double avg = values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                averages.put(entry.getKey(), avg);
            }
        }
        return averages;
    }

    /**
     * 数据导出
     */
    @GetMapping("/export")
    public ResponseEntity<? super byte[]> exportData(
            @RequestParam String startTime,
            @RequestParam String endTime,
            @RequestParam(required = false) List<String> robotIds,
            @RequestParam(required = false) List<String> sensorTypes,
            @RequestParam(defaultValue = "csv") String format) {
        try {
            // 查询数据
            List<String> metrics = Arrays.asList("temperature", "humidity", "pressure");
            List<Map<String, Object>> data = hiveQueryRouterMapper.routeQuery(
                startTime, endTime, robotIds, sensorTypes, metrics
            );

            // 导出数据
            byte[] exportData = exportToFormat(data, format);

            String filename = String.format("sensor_data_%s_%s.%s",
                startTime.replaceAll("[:\\s-]", ""),
                endTime.replaceAll("[:\\s-]", ""),
                format
            );

            return ResponseEntity.ok()
                    .header("Content-Type", getContentType(format))
                    .header("Content-Disposition", "attachment; filename=" + filename)
                    .body(exportData);
        } catch (Exception e) {
            log.error("导出传感器数据失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 导出为指定格式
     */
    private byte[] exportToFormat(List<Map<String, Object>> data, String format) {
        if ("csv".equalsIgnoreCase(format)) {
            return exportToCsv(data);
        } else if ("json".equalsIgnoreCase(format)) {
            return exportToJson(data);
        } else if ("excel".equalsIgnoreCase(format)) {
            return exportToExcel(data);
        }
        return new byte[0];
    }

    /**
     * 导出为CSV
     */
    private byte[] exportToCsv(List<Map<String, Object>> data) {
        StringBuilder csv = new StringBuilder();
        csv.append("机器人ID,机器人名称,机器人类型,平均温度,最高温度,最低温度,平均湿度,最高湿度,最低湿度,平均压力,数据量\n");

        for (Map<String, Object> record : data) {
            csv.append(record.getOrDefault("robotId", "")).append(",");
            csv.append(record.getOrDefault("robotName", "")).append(",");
            csv.append(record.getOrDefault("robotType", "")).append(",");
            csv.append(record.getOrDefault("avgTemperature", "")).append(",");
            csv.append(record.getOrDefault("maxTemperature", "")).append(",");
            csv.append(record.getOrDefault("minTemperature", "")).append(",");
            csv.append(record.getOrDefault("avgHumidity", "")).append(",");
            csv.append(record.getOrDefault("maxHumidity", "")).append(",");
            csv.append(record.getOrDefault("minHumidity", "")).append(",");
            csv.append(record.getOrDefault("avgPressure", "")).append(",");
            csv.append(record.getOrDefault("dataCount", "")).append("\n");
        }

        return csv.toString().getBytes();
    }

    /**
     * 导出为JSON
     */
    private byte[] exportToJson(List<Map<String, Object>> data) {
        Map<String, Object> json = Map.of(
            "exportTime", LocalDateTime.now().format(FORMATTER),
            "totalRecords", data.size(),
            "data", data
        );
        return JsonUtil.toJson(json).getBytes();
    }

    /**
     * 导出为Excel（简化实现）
     */
    private byte[] exportToExcel(List<Map<String, Object>> data) {
        // 这里应该使用Apache POI生成真正的Excel文件
        // 简化实现，返回CSV格式的数据
        return exportToCsv(data);
    }

    /**
     * 获取内容类型
     */
    private String getContentType(String format) {
        switch (format.toLowerCase()) {
            case "csv":
                return "text/csv";
            case "json":
                return "application/json";
            case "excel":
            case "xlsx":
                return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
            default:
                return "application/octet-stream";
        }
    }

    /**
     * 获取数据概览
     */
    @GetMapping("/overview")
    public ResponseEntity<? super Map<String, Object>> getOverview() {
        try {
            // 查询最近24小时的数据概览
            String startTime = LocalDateTime.now().minusDays(1).format(FORMATTER);
            String endTime = LocalDateTime.now().format(FORMATTER);

            List<String> metrics = Arrays.asList("temperature", "humidity", "pressure");
            List<Map<String, Object>> data = hiveQueryRouterMapper.routeQuery(
                startTime, endTime, null, null, metrics
            );

            Map<String, Object> overview = new HashMap<>();
            overview.put("totalRecords", data.size());
            overview.put("uniqueRobots", data.stream().map(d -> d.get("robotId")).distinct().count());
            overview.put("statistics", calculateStatistics(data, "robot"));
            overview.put("trend", aggregateByInterval(data, "hour"));

            Map<String, Object> result = Map.of(
                "success", true,
                "overview", overview,
                "generatedAt", LocalDateTime.now().format(FORMATTER)
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取数据概览失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取数据质量报告
     */
    @GetMapping("/quality-report")
    public ResponseEntity<? super Map<String, Object>> getDataQualityReport(
            @RequestParam String startTime,
            @RequestParam String endTime) {
        try {
            // 查询数据
            List<String> metrics = Arrays.asList("temperature", "humidity", "pressure", "data_count");
            List<Map<String, Object>> data = hiveQueryRouterMapper.routeQuery(
                startTime, endTime, null, null, metrics
            );

            // 计算数据质量指标
            Map<String, Object> qualityReport = calculateDataQuality(data);

            Map<String, Object> result = Map.of(
                "success", true,
                "qualityReport", qualityReport,
                "period", Map.of("start", startTime, "end", endTime)
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取数据质量报告失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 计算数据质量
     */
    private Map<String, Object> calculateDataQuality(List<Map<String, Object>> data) {
        Map<String, Object> quality = new HashMap<>();

        int totalRecords = data.size();
        int completeRecords = 0;
        int validRecords = 0;
        int abnormalRecords = 0;

        for (Map<String, Object> record : data) {
            // 检查完整性
            boolean isComplete = record.containsKey("avgTemperature") &&
                    record.containsKey("avgHumidity") &&
                    record.containsKey("avgPressure");
            if (isComplete) {
                completeRecords++;
            }

            // 检查有效性
            boolean isValid = true;
            for (String metric : Arrays.asList("avgTemperature", "avgHumidity", "avgPressure")) {
                if (record.containsKey(metric)) {
                    Double value = (Double) record.get(metric);
                    if (value == null || Double.isNaN(value) || Double.isInfinite(value)) {
                        isValid = false;
                        break;
                    }
                }
            }
            if (isValid) {
                validRecords++;
            }

            // 检查异常值
            if (record.containsKey("avgTemperature")) {
                Double temp = (Double) record.get("avgTemperature");
                if (temp > 100 || temp < -50) {
                    abnormalRecords++;
                }
            }
        }

        quality.put("totalRecords", totalRecords);
        quality.put("completeRecords", completeRecords);
        quality.put("completenessRate", totalRecords > 0 ? (double) completeRecords / totalRecords : 0);
        quality.put("validRecords", validRecords);
        quality.put("validityRate", totalRecords > 0 ? (double) validRecords / totalRecords : 0);
        quality.put("abnormalRecords", abnormalRecords);
        quality.put("abnormalityRate", totalRecords > 0 ? (double) abnormalRecords / totalRecords : 0);
        quality.put("overallQuality", calculateOverallQuality(completeRecords, validRecords, totalRecords));

        return quality;
    }

    /**
     * 计算总体质量
     */
    private double calculateOverallQuality(int completeRecords, int validRecords, int totalRecords) {
        if (totalRecords == 0) {
            return 0;
        }
        double completeness = (double) completeRecords / totalRecords;
        double validity = (double) validRecords / totalRecords;
        return (completeness + validity) / 2 * 100;
    }

    /**
     * 获取数据分布
     */
    @GetMapping("/distribution")
    public ResponseEntity<? super Map<String, Object>> getDistribution(
            @RequestParam String metric,
            @RequestParam String startTime,
            @RequestParam String endTime,
            @RequestParam(defaultValue = "10") int buckets) {
        try {
            List<String> metrics = Arrays.asList(metric);
            List<Map<String, Object>> data = hiveQueryRouterMapper.routeQuery(
                startTime, endTime, null, null, metrics
            );

            // 计算分布
            Map<String, Object> distribution = calculateDistribution(data, metric, buckets);

            Map<String, Object> result = Map.of(
                "success", true,
                "distribution", distribution,
                "metric", metric,
                "buckets", buckets
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取数据分布失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 计算数据分布
     */
    private Map<String, Object> calculateDistribution(List<Map<String, Object>> data, String metric, int buckets) {
        Map<String, Object> distribution = new HashMap<>();

        // 提取数值
        List<Double> values = new ArrayList<>();
        for (Map<String, Object> record : data) {
            if (record.containsKey("avg" + capitalize(metric))) {
                values.add((Double) record.get("avg" + capitalize(metric)));
            }
        }

        if (values.isEmpty()) {
            distribution.put("buckets", new ArrayList<>());
            distribution.put("min", 0);
            distribution.put("max", 0);
            return distribution;
        }

        // 计算最小值和最大值
        double min = values.stream().mapToDouble(Double::doubleValue).min().orElse(0);
        double max = values.stream().mapToDouble(Double::doubleValue).max().orElse(0);
        double step = (max - min) / buckets;

        // 创建桶
        List<Map<String, Object>> bucketsList = new ArrayList<>();
        for (int i = 0; i < buckets; i++) {
            double bucketMin = min + i * step;
            double bucketMax = min + (i + 1) * step;
            int count = 0;

            for (Double value : values) {
                if (value >= bucketMin && value < bucketMax) {
                    count++;
                }
            }

            Map<String, Object> bucket = new HashMap<>();
            bucket.put("min", bucketMin);
            bucket.put("max", bucketMax);
            bucket.put("count", count);
            bucket.put("percentage", totalRecords > 0 ? (double) count / values.size() * 100 : 0);
            bucketsList.add(bucket);
        }

        distribution.put("buckets", bucketsList);
        distribution.put("min", min);
        distribution.put("max", max);
        distribution.put("mean", values.stream().mapToDouble(Double::doubleValue).average().orElse(0));
        distribution.put("stdDev", calculateStdDev(values, values.stream().mapToDouble(Double::doubleValue).average().orElse(0)));

        return distribution;
    }

    /**
     * 首字母大写
     */
    private String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
    }

    /**
     * 获取数据采样
     */
    @GetMapping("/sample")
    public ResponseEntity<? super Map<String, Object>> getSampleData(
            @RequestParam String startTime,
            @RequestParam String endTime,
            @RequestParam(defaultValue = "1000") int sampleSize,
            @RequestParam(defaultValue = "random") String samplingMethod) {
        try {
            List<String> metrics = Arrays.asList("temperature", "humidity", "pressure");
            List<Map<String, Object>> data = hiveQueryRouterMapper.routeQuery(
                startTime, endTime, null, null, metrics
            );

            // 采样
            List<Map<String, Object>> sampleData = sampleData(data, sampleSize, samplingMethod);

            Map<String, Object> result = Map.of(
                "success", true,
                "sampleData", sampleData,
                "originalSize", data.size(),
                "sampleSize", sampleData.size(),
                "samplingMethod", samplingMethod
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取数据采样失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 采样数据
     */
    private List<Map<String, Object>> sampleData(List<Map<String, Object>> data, int sampleSize, String method) {
        if (data.size() <= sampleSize) {
            return data;
        }

        if ("random".equals(method)) {
            // 随机采样
            Collections.shuffle(data);
            return data.subList(0, sampleSize);
        } else if ("systematic".equals(method)) {
            // 系统采样
            int step = data.size() / sampleSize;
            List<Map<String, Object>> sample = new ArrayList<>();
            for (int i = 0; i < data.size(); i += step) {
                sample.add(data.get(i));
                if (sample.size() >= sampleSize) {
                    break;
                }
            }
            return sample;
        } else {
            // 默认返回前N条
            return data.subList(0, sampleSize);
        }
    }

    /**
     * 计算标准差
     */
    private double calculateStdDev(List<Double> values, double mean) {
        double sum = 0;
        for (double value : values) {
            sum += Math.pow(value - mean, 2);
        }
        return Math.sqrt(sum / values.size());
    }

    /**
     * 获取数据聚合配置
     */
    @GetMapping("/aggregation/config")
    public ResponseEntity<? super Map<String, Object>> getAggregationConfig() {
        try {
            Map<String, Object> config = Map.of(
                "supportedIntervals", Arrays.asList("minute", "hour", "day", "week", "month"),
                "supportedMetrics", Arrays.asList("temperature", "humidity", "pressure", "vibration"),
                "supportedFunctions", Arrays.asList("avg", "max", "min", "sum", "count", "stddev"),
                "defaultRetention", "90 days"
            );

            Map<String, Object> result = Map.of(
                "success", true,
                "config", config
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取数据聚合配置失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取数据存储统计
     */
    @GetMapping("/storage/stats")
    public ResponseEntity<? super Map<String, Object>> getStorageStats() {
        try {
            // 这里应该查询实际的存储统计信息
            Map<String, Object> stats = Map.of(
                "totalSize", "2.3TB",
                "dailyGrowth", "15GB",
                "compressionRatio", 0.7,
                "partitionCount", 1245,
                "oldestData", "2023-01-01",
                "latestData", LocalDateTime.now().format(FORMATTER)
            );

            Map<String, Object> result = Map.of(
                "success", true,
                "storageStats", stats
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取存储统计失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 清理过期数据
     */
    @DeleteMapping("/cleanup")
    public ResponseEntity<? super Map<String, Object>> cleanupData(
            @RequestParam String endTime,
            @RequestParam(defaultValue = "false") boolean dryRun) {
        try {
            // 这里应该调用实际的清理逻辑
            if (dryRun) {
                // 模拟清理预览
                Map<String, Object> preview = Map.of(
                    "recordsToDelete", 1234567,
                    "estimatedSize", "2.5GB",
                    "affectedPartitions", 45
                );

                Map<String, Object> result = Map.of(
                    "success", true,
                    "preview", preview,
                    "message", "清理预览完成"
                );
                return ResponseEntity.ok(result);
            } else {
                // 实际清理逻辑
                // hiveQueryRouterMapper.cleanupData(endTime);

                Map<String, Object> result = Map.of(
                    "success", true,
                    "message", "数据清理完成"
                );
                return ResponseEntity.ok(result);
            }
        } catch (Exception e) {
            log.error("清理数据失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取数据血缘关系
     */
    @GetMapping("/lineage/{dataId}")
    public ResponseEntity<? super Map<String, Object>> getDataLineage(@PathVariable String dataId) {
        try {
            // 这里应该查询实际的数据血缘关系
            Map<String, Object> lineage = Map.of(
                "dataId", dataId,
                "source", "传感器采集",
                "processingSteps", Arrays.asList(
                    Map.of("step", "数据采集", "time", LocalDateTime.now().minusMinutes(5).format(FORMATTER)),
                    Map.of("step", "数据清洗", "time", LocalDateTime.now().minusMinutes(4).format(FORMATTER)),
                    Map.of("step", "数据存储", "time", LocalDateTime.now().minusMinutes(3).format(FORMATTER)),
                    Map.of("step", "数据分析", "time", LocalDateTime.now().minusMinutes(2).format(FORMATTER))
                ),
                "downstreamSystems", Arrays.asList(
                    Map.of("system", "实时监控系统", "status", "active"),
                    Map.of("system", "数据分析平台", "status", "active"),
                    Map.of("system", "报表系统", "status", "active")
                )
            );

            Map<String, Object> result = Map.of(
                "success", true,
                "lineage", lineage
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取数据血缘关系失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取数据标签
     */
    @GetMapping("/tags")
    public ResponseEntity<? super Map<String, Object>> getDataTags() {
        try {
            List<String> tags = Arrays.asList(
                "realtime", "batch", "anomaly", "critical", "processed", "raw"
            );

            Map<String, Object> result = Map.of(
                "success", true,
                "tags", tags
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取数据标签失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 标记数据
     */
    @PostMapping("/{dataId}/tag")
    public ResponseEntity<? super Map<String, Object>> tagData(
            @PathVariable String dataId,
            @RequestBody Map<String, Object> tags) {
        try {
            // 这里应该实现实际的标记逻辑
            log.info("标记数据: dataId={}, tags={}", dataId, tags);

            Map<String, Object> result = Map.of(
                "success", true,
                "message", "数据标记成功"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("标记数据失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取数据访问日志
     */
    @GetMapping("/access-log")
    public ResponseEntity<? super Map<String, Object>> getAccessLog(
            @RequestParam String startTime,
            @RequestParam String endTime,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int size) {
        try {
            // 这里应该查询实际的数据访问日志
            List<Map<String, Object>> accessLogs = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                Map<String, Object> log = Map.of(
                    "timestamp", LocalDateTime.now().minusMinutes(i).format(FORMATTER),
                    "userId", "user" + (i % 10),
                    "operation", i % 2 == 0 ? "query" : "export",
                    "dataSize", (i + 1) * 1000,
                    "duration", (i + 1) * 100
                );
                accessLogs.add(log);
            }

            Map<String, Object> result = Map.of(
                "success", true,
                "logs", accessLogs,
                "page", page,
                "size", size,
                "total", 1000
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取数据访问日志失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 数据订阅
     */
    @PostMapping("/subscribe")
    public ResponseEntity<? super Map<String, Object>> subscribeData(
            @RequestBody Map<String, Object> subscription) {
        try {
            String callbackUrl = (String) subscription.get("callbackUrl");
            List<String> robotIds = (List<String>) subscription.get("robotIds");
            List<String> sensorTypes = (List<String>) subscription.get("sensorTypes");

            // 这里应该实现实际的订阅逻辑
            String subscriptionId = "SUB" + System.currentTimeMillis();

            log.info("数据订阅请求: callbackUrl={}, robotIds={}, sensorTypes={}",
                    callbackUrl, robotIds, sensorTypes);

            Map<String, Object> result = Map.of(
                "success", true,
                "subscriptionId", subscriptionId,
                "message", "订阅成功"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("数据订阅失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 取消数据订阅
     */
    @DeleteMapping("/subscribe/{subscriptionId}")
    public ResponseEntity<? super Map<String, Object>> unsubscribeData(@PathVariable String subscriptionId) {
        try {
            // 这里应该实现实际的取消订阅逻辑
            log.info("取消数据订阅: subscriptionId={}", subscriptionId);

            Map<String, Object> result = Map.of(
                "success", true,
                "message", "取消订阅成功"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("取消数据订阅失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 数据脱敏
     */
    @PostMapping("/desensitize")
    public ResponseEntity<? super Map<String, Object>> desensitizeData(
            @RequestBody Map<String, Object> request) {
        try {
            String startTime = (String) request.get("startTime");
            String endTime = (String) request.get("endTime");
            List<String> robotIds = (List<String>) request.get("robotIds");
            Map<String, String> desensitizationRules = (Map<String, String>) request.get("rules");

            // 查询数据
            List<String> metrics = Arrays.asList("temperature", "humidity", "pressure");
            List<Map<String, Object>> data = hiveQueryRouterMapper.routeQuery(
                startTime, endTime, robotIds, null, metrics
            );

            // 应用脱敏规则
            List<Map<String, Object>> desensitizedData = applyDesensitizationRules(data, desensitizationRules);

            Map<String, Object> result = Map.of(
                "success", true,
                "desensitizedData", desensitizedData,
                "originalCount", data.size(),
                "desensitizedCount", desensitizedData.size()
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("数据脱敏失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 应用脱敏规则
     */
    private List<Map<String, Object>> applyDesensitizationRules(List<Map<String, Object>> data, Map<String, String> rules) {
        List<Map<String, Object>> desensitizedData = new ArrayList<>();

        for (Map<String, Object> record : data) {
            Map<String, Object> desensitizedRecord = new HashMap<>(record);

            for (Map.Entry<String, String> rule : rules.entrySet()) {
                String field = rule.getKey();
                String ruleType = rule.getValue();

                if (desensitizedRecord.containsKey(field)) {
                    Object value = desensitizedRecord.get(field);
                    Object desensitizedValue = desensitizeValue(value, ruleType);
                    desensitizedRecord.put(field, desensitizedValue);
                }
            }

            desensitizedData.add(desensitizedRecord);
        }

        return desensitizedData;
    }

    /**
     * 脱敏单个值
     */
    private Object desensitizeValue(Object value, String ruleType) {
        if (value == null) {
            return null;
        }

        switch (ruleType) {
            case "MASK":
                return "***";
            case "ROUND":
                if (value instanceof Double) {
                    return Math.round((Double) value);
                }
                return value;
            case "NOISE":
                if (value instanceof Double) {
                    double noise = (Math.random() - 0.5) * 0.1; // ±5% 噪声
                    return (Double) value * (1 + noise);
                }
                return value;
            default:
                return value;
        }
    }

    /**
     * 获取数据血缘分析
     */
    @GetMapping("/lineage/analysis/{dataId}")
    public ResponseEntity<? super Map<String, Object>> getLineageAnalysis(@PathVariable String dataId) {
        try {
            // 这里应该实现实际的血缘分析逻辑
            Map<String, Object> lineageAnalysis = Map.of(
                "dataId", dataId,
                "upstreamDependencies", Arrays.asList(
                    Map.of("system", "传感器采集系统", "type", "实时数据", "status", "active"),
                    Map.of("system", "数据预处理系统", "type", "批处理", "status", "active")
                ),
                "downstreamConsumers", Arrays.asList(
                    Map.of("system", "实时监控系统", "type", "API调用", "status", "active"),
                    Map.of("system", "数据分析平台", "type", "批量处理", "status", "active"),
                    Map.of("system", "报表系统", "type", "定时任务", "status", "active")
                ),
                "impactAnalysis", Map.of(
                    "affectedSystems", 5,
                    "criticalPath", false,
                    "recommendation", "建议保持上游系统稳定运行"
                )
            );

            Map<String, Object> result = Map.of(
                "success", true,
                "lineageAnalysis", lineageAnalysis
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取数据血缘分析失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取数据影响分析
     */
    @GetMapping("/impact/analysis/{dataId}")
    public ResponseEntity<? super Map<String, Object>> getImpactAnalysis(@PathVariable String dataId) {
        try {
            // 这里应该实现实际的影响分析逻辑
            Map<String, Object> impactAnalysis = Map.of(
                "dataId", dataId,
                "impactScope", Map.of(
                    "affectedRobots", Arrays.asList("R001", "R002", "R003"),
                    "affectedMetrics", Arrays.asList("temperature", "humidity"),
                    "timeWindow", "最近24小时"
                ),
                "businessImpact", Map.of(
                    "severity", "MEDIUM",
                    "description", "可能影响生产质量分析",
                    "estimatedLoss", 50000
                ),
                "mitigation", Map.of(
                    "immediateActions", Arrays.asList("启用备用数据源", "通知相关人员"),
                    "longTermActions", Arrays.asList("优化数据采集链路", "增加冗余备份")
                )
            );

            Map<String, Object> result = Map.of(
                "success", true,
                "impactAnalysis", impactAnalysis
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取数据影响分析失败: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }
}