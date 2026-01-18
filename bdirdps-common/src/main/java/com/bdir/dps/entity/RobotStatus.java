package com.bdir.dps.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * 机器人状态实体类
 * 用于存储机器人的实时状态信息
 */
@Data
@Accessors(chain = true)
public class RobotStatus {

    /**
     * 机器人ID
     */
    private String robotId;

    /**
     * 机器人名称
     */
    private String robotName;

    /**
     * 机器人类型
     */
    private String robotType;

    /**
     * 在线状态
     */
    private String status = "OFFLINE";

    /**
     * 位置信息
     */
    private Position position;

    /**
     * 传感器数据
     */
    private Map<String, Double> sensorData;

    /**
     * 任务状态
     */
    private String taskStatus = "IDLE";

    /**
     * 当前任务ID
     */
    private String currentTaskId;

    /**
     * 电池电量
     */
    private Double batteryLevel;

    /**
     * 温度
     */
    private Double temperature;

    /**
     * 错误码
     */
    private String errorCode;

    /**
     * 错误描述
     */
    private String errorDescription;

    /**
     * 最后更新时间
     */
    private LocalDateTime lastUpdateTime;

    /**
     * 位置内部类
     */
    @Data
    @Accessors(chain = true)
    public static class Position {
        private Double x;
        private Double y;
        private Double z;
        private Double rotation;

        @Override
        public String toString() {
            return String.format("Position{x=%.2f, y=%.2f, z=%.2f, rotation=%.2f}", x, y, z, rotation);
        }
    }

    /**
     * 检查是否有异常指标
     */
    public boolean hasAbnormalMetrics() {
        if (sensorData == null) {
            return false;
        }
        // 检查温度是否过高
        if (temperature != null && temperature > 80.0) {
            return true;
        }
        // 检查电池电量是否过低
        if (batteryLevel != null && batteryLevel < 10.0) {
            return true;
        }
        // 检查其他传感器数据是否异常
        return sensorData.entrySet().stream()
                .anyMatch(entry -> {
                    String key = entry.getKey();
                    Double value = entry.getValue();
                    // 简单的异常判断逻辑
                    if (key.contains("temperature") && value > 100) {
                        return true;
                    }
                    if (key.contains("pressure") && (value < 0 || value > 1000)) {
                        return true;
                    }
                    return false;
                });
    }

    /**
     * 获取最大异常等级
     */
    public String getMaxSeverity() {
        if (!hasAbnormalMetrics()) {
            return "NORMAL";
        }
        // 简单的等级判断
        if (temperature != null && temperature > 100) {
            return "CRITICAL";
        }
        if (batteryLevel != null && batteryLevel < 5) {
            return "HIGH";
        }
        return "MEDIUM";
    }

    /**
     * 获取异常描述
     */
    public String getAbnormalDescription() {
        StringBuilder desc = new StringBuilder();
        if (temperature != null && temperature > 80.0) {
            desc.append("Temperature too high: ").append(temperature).append("°C; ");
        }
        if (batteryLevel != null && batteryLevel < 10.0) {
            desc.append("Battery too low: ").append(batteryLevel).append("%; ");
        }
        return desc.toString();
    }
}