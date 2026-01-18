package com.bdir.dps.controller;

import com.bdir.dps.entity.RobotCommand;
import com.bdir.dps.entity.RobotStatus;
import com.bdir.dps.service.RobotControlService;
import com.bdir.dps.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 机器人控制控制器
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/robots")
public class RobotController {

    @Autowired
    private RobotControlService robotControlService;

    /**
     * 获取机器人状态
     */
    @GetMapping("/{robotId}/status")
    public ResponseEntity<? super RobotStatus> getRobotStatus(@PathVariable String robotId) {
        try {
            RobotStatus status = robotControlService.getRobotStatus(robotId);
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            log.error("获取机器人状态失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 批量获取机器人状态
     */
    @PostMapping("/status/batch")
    public ResponseEntity<? super List<RobotStatus>> getRobotsStatus(@RequestBody List<String> robotIds) {
        try {
            CompletableFuture<List<RobotStatus>> future = robotControlService.getRobotsStatus(robotIds);
            return ResponseEntity.ok(future.get());
        } catch (Exception e) {
            log.error("批量获取机器人状态失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 发送控制指令
     */
    @PostMapping("/{robotId}/commands")
    public ResponseEntity<? super Map<String, Object>> sendCommand(
            @PathVariable String robotId,
            @RequestBody RobotCommand command) {
        try {
            CompletableFuture<Boolean> future = robotControlService.sendCommand(robotId, command);
            boolean success = future.get();

            Map<String, Object> result = Map.of(
                "success", success,
                "commandId", command.getCommandId(),
                "message", success ? "指令发送成功" : "指令发送失败"
            );

            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("发送指令失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取所有在线机器人
     */
    @GetMapping("/online")
    public ResponseEntity<? super List<String>> getOnlineRobots() {
        try {
            List<String> onlineRobots = robotControlService.getOnlineRobots();
            return ResponseEntity.ok(onlineRobots);
        } catch (Exception e) {
            log.error("获取在线机器人失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 获取系统统计信息
     */
    @GetMapping("/stats")
    public ResponseEntity<? super Map<String, Object>> getSystemStats() {
        try {
            Map<String, Object> stats = robotControlService.getSystemStats();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            log.error("获取系统统计失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 健康检查
     */
    @GetMapping("/health")
    public ResponseEntity<? super Map<String, Object>> healthCheck() {
        try {
            boolean healthy = robotControlService.healthCheck();
            Map<String, Object> result = Map.of(
                "status", healthy ? "UP" : "DOWN",
                "timestamp", System.currentTimeMillis()
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("健康检查失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("status", "DOWN", "error", e.getMessage())
            );
        }
    }

    /**
     * 处理机器人心跳
     */
    @PostMapping("/{robotId}/heartbeat")
    public ResponseEntity<Void> handleHeartbeat(
            @PathVariable String robotId,
            @RequestBody RobotStatus status) {
        try {
            robotControlService.handleRobotHeartbeat(robotId, status);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.error("处理心跳失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 重试失败的指令
     */
    @PostMapping("/commands/{commandId}/retry")
    public ResponseEntity<? super Map<String, Object>> retryCommand(
            @PathVariable String commandId) {
        try {
            CompletableFuture<Boolean> future = robotControlService.retryCommand(commandId);
            boolean success = future.get();

            Map<String, Object> result = Map.of(
                "success", success,
                "message", success ? "重试成功" : "重试失败"
            );

            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("重试指令失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 取消指令
     */
    @DeleteMapping("/commands/{commandId}")
    public ResponseEntity<? super Map<String, Object>> cancelCommand(
            @PathVariable String commandId) {
        try {
            boolean cancelled = robotControlService.cancelCommand(commandId);
            Map<String, Object> result = Map.of(
                "success", cancelled,
                "message", cancelled ? "指令已取消" : "指令无法取消"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("取消指令失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取机器人历史指令
     */
    @GetMapping("/{robotId}/commands/history")
    public ResponseEntity<? super List<RobotCommand>> getCommandHistory(
            @PathVariable String robotId,
            @RequestParam(defaultValue = "10") int limit) {
        try {
            List<RobotCommand> history = robotControlService.getRobotCommandHistory(robotId, limit);
            return ResponseEntity.ok(history);
        } catch (Exception e) {
            log.error("获取指令历史失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 批量发送指令
     */
    @PostMapping("/commands/batch")
    public ResponseEntity<? super Map<String, Boolean>> sendBatchCommands(
            @RequestBody Map<String, RobotCommand> commands) {
        try {
            CompletableFuture<Map<String, Boolean>> future = robotControlService.sendBatchCommands(commands);
            Map<String, Boolean> results = future.get();
            return ResponseEntity.ok(results);
        } catch (Exception e) {
            log.error("批量发送指令失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 暂停机器人
     */
    @PostMapping("/{robotId}/pause")
    public ResponseEntity<? super Map<String, Object>> pauseRobot(@PathVariable String robotId) {
        try {
            CompletableFuture<Boolean> future = robotControlService.pauseRobot(robotId);
            boolean success = future.get();
            Map<String, Object> result = Map.of(
                "success", success,
                "message", success ? "机器人已暂停" : "暂停失败"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("暂停机器人失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 恢复机器人
     */
    @PostMapping("/{robotId}/resume")
    public ResponseEntity<? super Map<String, Object>> resumeRobot(@PathVariable String robotId) {
        try {
            CompletableFuture<Boolean> future = robotControlService.resumeRobot(robotId);
            boolean success = future.get();
            Map<String, Object> result = Map.of(
                "success", success,
                "message", success ? "机器人已恢复" : "恢复失败"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("恢复机器人失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 停止机器人
     */
    @PostMapping("/{robotId}/stop")
    public ResponseEntity<? super Map<String, Object>> stopRobot(@PathVariable String robotId) {
        try {
            CompletableFuture<Boolean> future = robotControlService.stopRobot(robotId);
            boolean success = future.get();
            Map<String, Object> result = Map.of(
                "success", success,
                "message", success ? "机器人已停止" : "停止失败"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("停止机器人失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 重启机器人
     */
    @PostMapping("/{robotId}/restart")
    public ResponseEntity<? super Map<String, Object>> restartRobot(@PathVariable String robotId) {
        try {
            CompletableFuture<Boolean> future = robotControlService.restartRobot(robotId);
            boolean success = future.get();
            Map<String, Object> result = Map.of(
                "success", success,
                "message", success ? "机器人已重启" : "重启失败"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("重启机器人失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取机器人配置
     */
    @GetMapping("/{robotId}/config")
    public ResponseEntity<? super Map<String, Object>> getRobotConfig(@PathVariable String robotId) {
        try {
            Map<String, Object> config = robotControlService.getRobotConfig(robotId);
            return ResponseEntity.ok(config);
        } catch (Exception e) {
            log.error("获取机器人配置失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 更新机器人配置
     */
    @PutMapping("/{robotId}/config")
    public ResponseEntity<? super Map<String, Object>> updateRobotConfig(
            @PathVariable String robotId,
            @RequestBody Map<String, Object> config) {
        try {
            CompletableFuture<Boolean> future = robotControlService.updateRobotConfig(robotId, config);
            boolean success = future.get();
            Map<String, Object> result = Map.of(
                "success", success,
                "message", success ? "配置已更新" : "配置更新失败"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("更新机器人配置失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取机器人日志
     */
    @GetMapping("/{robotId}/logs")
    public ResponseEntity<? super List<String>> getRobotLogs(
            @PathVariable String robotId,
            @RequestParam(defaultValue = "100") int lines) {
        try {
            List<String> logs = robotControlService.getRobotLogs(robotId, lines);
            return ResponseEntity.ok(logs);
        } catch (Exception e) {
            log.error("获取机器人日志失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 获取机器人性能指标
     */
    @GetMapping("/{robotId}/metrics")
    public ResponseEntity<? super Map<String, Object>> getRobotMetrics(@PathVariable String robotId) {
        try {
            Map<String, Object> metrics = robotControlService.getRobotMetrics(robotId);
            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            log.error("获取机器人性能指标失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 执行机器人自检
     */
    @PostMapping("/{robotId}/self-check")
    public ResponseEntity<? super Map<String, Object>> runSelfCheck(@PathVariable String robotId) {
        try {
            CompletableFuture<Map<String, Object>> future = robotControlService.runRobotSelfCheck(robotId);
            Map<String, Object> result = future.get();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("执行机器人自检失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 获取机器人API状态
     */
    @GetMapping("/api/status")
    public ResponseEntity<? super Map<String, Object>> getApiStatus() {
        try {
            boolean available = robotControlService.isRobotApiAvailable();
            String version = robotControlService.getRobotFirmwareVersion("R001"); // 示例

            Map<String, Object> result = Map.of(
                "available", available,
                "firmwareVersion", version,
                "timestamp", System.currentTimeMillis()
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取API状态失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 同步机器人时间
     */
    @PostMapping("/{robotId}/sync-time")
    public ResponseEntity<? super Map<String, Object>> syncRobotTime(@PathVariable String robotId) {
        try {
            CompletableFuture<Boolean> future = robotControlService.syncRobotTime(robotId);
            boolean success = future.get();
            Map<String, Object> result = Map.of(
                "success", success,
                "message", success ? "时间同步成功" : "时间同步失败"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("同步机器人时间失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取机器人诊断信息
     */
    @GetMapping("/{robotId}/diagnostics")
    public ResponseEntity<? super Map<String, Object>> getRobotDiagnostics(@PathVariable String robotId) {
        try {
            Map<String, Object> diagnostics = robotControlService.getRobotDiagnostics(robotId);
            return ResponseEntity.ok(diagnostics);
        } catch (Exception e) {
            log.error("获取机器人诊断信息失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 校准机器人传感器
     */
    @PostMapping("/{robotId}/calibrate-sensors")
    public ResponseEntity<? super Map<String, Object>> calibrateSensors(@PathVariable String robotId) {
        try {
            CompletableFuture<Boolean> future = robotControlService.calibrateRobotSensors(robotId);
            boolean success = future.get();
            Map<String, Object> result = Map.of(
                "success", success,
                "message", success ? "传感器校准成功" : "传感器校准失败"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("校准机器人传感器失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取机器人传感器状态
     */
    @GetMapping("/{robotId}/sensors/status")
    public ResponseEntity<? super Map<String, Object>> getSensorStatus(@PathVariable String robotId) {
        try {
            Map<String, Object> sensorStatus = robotControlService.getRobotSensorStatus(robotId);
            return ResponseEntity.ok(sensorStatus);
        } catch (Exception e) {
            log.error("获取机器人传感器状态失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 设置机器人工作模式
     */
    @PostMapping("/{robotId}/work-mode")
    public ResponseEntity<? super Map<String, Object>> setWorkMode(
            @PathVariable String robotId,
            @RequestParam String mode) {
        try {
            CompletableFuture<Boolean> future = robotControlService.setRobotWorkMode(robotId, mode);
            boolean success = future.get();
            Map<String, Object> result = Map.of(
                "success", success,
                "message", success ? "工作模式已设置" : "设置工作模式失败"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("设置机器人工作模式失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取机器人工作模式
     */
    @GetMapping("/{robotId}/work-mode")
    public ResponseEntity<? super Map<String, Object>> getWorkMode(@PathVariable String robotId) {
        try {
            String mode = robotControlService.getRobotWorkMode(robotId);
            Map<String, Object> result = Map.of("workMode", mode);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取机器人工作模式失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 调度机器人任务
     */
    @PostMapping("/{robotId}/schedule-task")
    public ResponseEntity<? super Map<String, Object>> scheduleTask(
            @PathVariable String robotId,
            @RequestParam String taskType,
            @RequestBody Map<String, Object> taskParams) {
        try {
            CompletableFuture<Boolean> future = robotControlService.scheduleRobotTask(robotId, taskType, taskParams);
            boolean success = future.get();
            Map<String, Object> result = Map.of(
                "success", success,
                "message", success ? "任务已调度" : "任务调度失败"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("调度机器人任务失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取计划任务列表
     */
    @GetMapping("/{robotId}/scheduled-tasks")
    public ResponseEntity<? super List<Map<String, Object>>> getScheduledTasks(@PathVariable String robotId) {
        try {
            List<Map<String, Object>> tasks = robotControlService.getScheduledTasks(robotId);
            return ResponseEntity.ok(tasks);
        } catch (Exception e) {
            log.error("获取计划任务列表失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 获取机器人工作日志
     */
    @GetMapping("/{robotId}/work-log")
    public ResponseEntity<? super List<Map<String, Object>>> getWorkLog(
            @PathVariable String robotId,
            @RequestParam String startTime,
            @RequestParam String endTime) {
        try {
            // 解析时间参数
            LocalDateTime start = LocalDateTime.parse(startTime);
            LocalDateTime end = LocalDateTime.parse(endTime);

            List<Map<String, Object>> workLog = robotControlService.getRobotWorkLog(robotId, start, end);
            return ResponseEntity.ok(workLog);
        } catch (Exception e) {
            log.error("获取机器人工作日志失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 导出机器人数据
     */
    @GetMapping("/{robotId}/export")
    public ResponseEntity<? super byte[]> exportRobotData(
            @PathVariable String robotId,
            @RequestParam String format,
            @RequestParam String startTime,
            @RequestParam String endTime) {
        try {
            // 解析时间参数
            LocalDateTime start = LocalDateTime.parse(startTime);
            LocalDateTime end = LocalDateTime.parse(endTime);

            byte[] data = robotControlService.exportRobotData(robotId, format, start, end);

            return ResponseEntity.ok()
                    .header("Content-Type", "application/octet-stream")
                    .header("Content-Disposition", "attachment; filename=robot_" + robotId + "_data." + format)
                    .body(data);
        } catch (Exception e) {
            log.error("导出机器人数据失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 执行机器人维护操作
     */
    @PostMapping("/{robotId}/maintenance")
    public ResponseEntity<? super Map<String, Object>> performMaintenance(
            @PathVariable String robotId,
            @RequestParam String maintenanceType) {
        try {
            CompletableFuture<Boolean> future = robotControlService.performMaintenance(robotId, maintenanceType);
            boolean success = future.get();
            Map<String, Object> result = Map.of(
                "success", success,
                "message", success ? "维护操作成功" : "维护操作失败"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("执行机器人维护操作失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取机器人维护历史
     */
    @GetMapping("/{robotId}/maintenance-history")
    public ResponseEntity<? super List<Map<String, Object>>> getMaintenanceHistory(@PathVariable String robotId) {
        try {
            List<Map<String, Object>> history = robotControlService.getMaintenanceHistory(robotId);
            return ResponseEntity.ok(history);
        } catch (Exception e) {
            log.error("获取机器人维护历史失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 重置机器人
     */
    @PostMapping("/{robotId}/reset")
    public ResponseEntity<? super Map<String, Object>> resetRobot(@PathVariable String robotId) {
        try {
            CompletableFuture<Boolean> future = robotControlService.resetRobot(robotId);
            boolean success = future.get();
            Map<String, Object> result = Map.of(
                "success", success,
                "message", success ? "机器人已重置" : "重置失败"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("重置机器人失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 升级机器人固件
     */
    @PostMapping("/{robotId}/firmware/upgrade")
    public ResponseEntity<? super Map<String, Object>> upgradeFirmware(
            @PathVariable String robotId,
            @RequestParam String version) {
        try {
            CompletableFuture<Boolean> future = robotControlService.upgradeRobotFirmware(robotId, version);
            boolean success = future.get();
            Map<String, Object> result = Map.of(
                "success", success,
                "message", success ? "固件升级成功" : "固件升级失败"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("升级机器人固件失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 获取机器人固件版本
     */
    @GetMapping("/{robotId}/firmware/version")
    public ResponseEntity<? super Map<String, Object>> getFirmwareVersion(@PathVariable String robotId) {
        try {
            String version = robotControlService.getRobotFirmwareVersion(robotId);
            Map<String, Object> result = Map.of("firmwareVersion", version != null ? version : "unknown");
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("获取机器人固件版本失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 广播消息给所有在线机器人
     */
    @PostMapping("/broadcast")
    public ResponseEntity<? super Map<String, Object>> broadcastToAll(
            @RequestParam String messageType,
            @RequestBody Map<String, Object> payload) {
        try {
            CompletableFuture<Integer> future = robotControlService.broadcastToAllOnline(messageType, payload);
            int successCount = future.get();

            Map<String, Object> result = Map.of(
                "success", true,
                "message", String.format("广播完成，成功%d个机器人", successCount),
                "successCount", successCount
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("广播消息失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 清除缓存
     */
    @DeleteMapping("/cache")
    public ResponseEntity<? super Map<String, Object>> clearCache() {
        try {
            robotControlService.clearCache();
            Map<String, Object> result = Map.of(
                "success", true,
                "message", "缓存已清除"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("清除缓存失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 清除指定机器人缓存
     */
    @DeleteMapping("/{robotId}/cache")
    public ResponseEntity<? super Map<String, Object>> clearRobotCache(@PathVariable String robotId) {
        try {
            robotControlService.clearRobotCache(robotId);
            Map<String, Object> result = Map.of(
                "success", true,
                "message", "机器人缓存已清除"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("清除机器人缓存失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 下载机器人日志文件
     */
    @GetMapping("/{robotId}/logs/download/{logFileName}")
    public ResponseEntity<? super byte[]> downloadLogFile(
            @PathVariable String robotId,
            @PathVariable String logFileName) {
        try {
            byte[] logData = robotControlService.downloadRobotLogFile(robotId, logFileName);

            return ResponseEntity.ok()
                    .header("Content-Type", "text/plain")
                    .header("Content-Disposition", "attachment; filename=" + logFileName)
                    .body(logData);
        } catch (Exception e) {
            log.error("下载机器人日志文件失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 取消计划任务
     */
    @DeleteMapping("/{robotId}/scheduled-tasks/{taskId}")
    public ResponseEntity<? super Map<String, Object>> cancelScheduledTask(
            @PathVariable String robotId,
            @PathVariable String taskId) {
        try {
            CompletableFuture<Boolean> future = robotControlService.cancelScheduledTask(robotId, taskId);
            boolean success = future.get();
            Map<String, Object> result = Map.of(
                "success", success,
                "message", success ? "计划任务已取消" : "取消计划任务失败"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("取消计划任务失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }

    /**
     * 设置机器人状态监听器（WebSocket）
     */
    @GetMapping("/{robotId}/status/stream")
    public void streamRobotStatus(@PathVariable String robotId) {
        // WebSocket连接将通过WebSocketConfig处理
        // 这里只是一个占位符，实际状态推送通过WebSocket实现
        log.info("WebSocket连接请求: robotId={}", robotId);
    }

    /**
     * 获取指令缓存
     */
    @GetMapping("/commands/cache")
    public ResponseEntity<? super Map<String, Object>> getCommandCache() {
        try {
            Map<String, RobotCommand> cache = robotControlService.getCommandCache();
            return ResponseEntity.ok(Map.of("commands", cache));
        } catch (Exception e) {
            log.error("获取指令缓存失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 获取指定指令
     */
    @GetMapping("/commands/{commandId}")
    public ResponseEntity<? super RobotCommand> getCommand(@PathVariable String commandId) {
        try {
            RobotCommand command = robotControlService.getCommand(commandId);
            if (command != null) {
                return ResponseEntity.ok(command);
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            log.error("获取指令失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 获取所有机器人状态
     */
    @GetMapping("/status/all")
    public ResponseEntity<? super Map<String, RobotStatus>> getAllRobotStatus() {
        try {
            Map<String, RobotStatus> allStatus = robotControlService.getAllRobotStatus();
            return ResponseEntity.ok(allStatus);
        } catch (Exception e) {
            log.error("获取所有机器人状态失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * 设置机器人状态监听器
     */
    @PostMapping("/{robotId}/status/listener")
    public ResponseEntity<? super Map<String, Object>> setStatusListener(
            @PathVariable String robotId,
            @RequestBody(required = false) Map<String, Object> listenerConfig) {
        try {
            robotControlService.setRobotStatusListener(robotId, status -> {
                log.info("机器人状态变更通知: robotId={}, status={}", robotId, status.getStatus());
                // 这里可以实现自定义的状态变更处理逻辑
            });

            Map<String, Object> result = Map.of(
                "success", true,
                "message", "状态监听器已设置"
            );
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("设置机器人状态监听器失败: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                Map.of("success", false, "message", e.getMessage())
            );
        }
    }
}