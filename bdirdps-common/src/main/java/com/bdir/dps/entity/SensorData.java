package com.bdir.dps.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * 传感器数据实体类
 * 用于存储各类传感器采集的数据
 */
@Data
@Accessors(chain = true)
public class SensorData {

    /**
     * 数据ID
     */
    private String dataId;

    /**
     * 机器人ID
     */
    @NotBlank(message = "机器人ID不能为空")
    private String robotId;

    /**
     * 传感器ID
     */
    @NotBlank(message = "传感器ID不能为空")
    private String sensorId;

    /**
     * 传感器类型
     */
    @NotBlank(message = "传感器类型不能为空")
    private String sensorType;

    /**
     * 采集时间
     */
    @NotNull(message = "采集时间不能为空")
    private LocalDateTime timestamp;

    /**
     * 传感器数值
     */
    private Map<String, Double> metrics;

    /**
     * 位置信息
     */
    private Location location;

    /**
     * 单位
     */
    private String unit;

    /**
     * 精度
     */
    private Double precision;

    /**
     * 状态
     */
    private String status = "NORMAL";

    /**
     * 原始数据
     */
    private String rawData;

    /**
     * 校验状态
     */
    private Boolean valid = true;

    /**
     * 备注
     */
    private String remark;

    /**
     * 位置内部类
     */
    @Data
    @Accessors(chain = true)
    public static class Location {
        private Double x;
        private Double y;
        private Double z;
        private String coordinateSystem = "CARTESIAN";

        public static Location of(double x, double y, double z) {
            Location loc = new Location();
            loc.setX(x);
            loc.setY(y);
            loc.setZ(z);
            return loc;
        }
    }

    /**
     * 快速创建传感器数据
     */
    public static SensorData create(String robotId, String sensorId, String sensorType,
                                  Map<String, Double> metrics) {
        SensorData data = new SensorData();
        data.setRobotId(robotId);
        data.setSensorId(sensorId);
        data.setSensorType(sensorType);
        data.setTimestamp(LocalDateTime.now());
        data.setMetrics(metrics);
        return data;
    }

    /**
     * 获取指定指标的值
     */
    public Double getMetricValue(String metricName) {
        return metrics != null ? metrics.get(metricName) : null;
    }

    /**
     * 设置指标值
     */
    public SensorData setMetricValue(String metricName, Double value) {
        if (this.metrics == null) {
            this.metrics = new java.util.HashMap<>();
        }
        this.metrics.put(metricName, value);
        return this;
    }

    /**
     * 检查数据是否有效
     */
    public boolean isValid() {
        if (metrics == null || metrics.isEmpty()) {
            return false;
        }
        // 检查数值是否在合理范围内
        for (Map.Entry<String, Double> entry : metrics.entrySet()) {
            Double value = entry.getValue();
            if (value == null || Double.isNaN(value) || Double.isInfinite(value)) {
                return false;
            }
        }
        return valid;
    }
}