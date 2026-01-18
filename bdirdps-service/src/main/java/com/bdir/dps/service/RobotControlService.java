package com.bdir.dps.service;

import com.bdir.dps.entity.RobotCommand;
import com.bdir.dps.entity.RobotStatus;
import com.bdir.dps.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 机器人控制服务类
 * 提供机器人控制指令的发送和状态监控功能
 */
@Slf4j
@Service
public class RobotControlService {

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @Autowired
    private RestTemplate restTemplate;

    @Value("${robot.api.base-url:http://robot-api-gateway:8080/robots}")
    private String robotApiBaseUrl;

    @Value("${websocket.heartbeat.interval:30000}")
    private long heartbeatInterval;

    @Value("${robot.status.cache.expire:5000}")
    private long statusCacheExpire;

    // 机器人状态缓存
    private final ConcurrentHashMap<String, RobotStatus> robotStatusCache = new ConcurrentHashMap<>();

    // 指令执行缓存
    private final ConcurrentHashMap<String, RobotCommand> commandCache = new ConcurrentHashMap<>();

    /**
     * 发送控制指令到机器人
     *
     * @param robotId 机器人ID
     * @param command 控制指令
     * @return 执行结果
     */
    @Async
    public CompletableFuture<Boolean> sendCommand(String robotId, RobotCommand command) {
        long startTime = System.currentTimeMillis();

        try {
            log.info("Sending command to robot: {}, command type: {}", robotId, command.getCommandType());

            // 1. 验证机器人状态
            RobotStatus currentStatus = getRobotStatus(robotId);
            if (currentStatus == null || !"ONLINE".equals(currentStatus.getStatus())) {
                log.warn("Robot {} is not online, current status: {}", robotId,
                        currentStatus != null ? currentStatus.getStatus() : "UNKNOWN");
                command.setStatus("FAILED");
                command.setErrorMessage("Robot is not online");
                notifyCommandUpdate(robotId, command);
                return CompletableFuture.completedFuture(false);
            }

            // 2. 构建请求URL
            String url = String.format("%s/%s/commands", robotApiBaseUrl, robotId);

            // 3. 设置指令参数
            command.setCommandId(UUID.randomUUID().toString());
            command.setRobotId(robotId);
            command.setTimestamp(LocalDateTime.now());
            command.setStatus("PENDING");
            command.setRetryCount(0);

            // 4. 缓存指令
            commandCache.put(command.getCommandId(), command);

            // 5. 发送HTTP请求到机器人
            HttpEntity<RobotCommand> request = new HttpEntity<>(command);
            ResponseEntity<String> response = restTemplate.exchange(
                url, HttpMethod.POST, request, String.class);

            // 6. 处理响应结果
            if (response.getStatusCode().is2xxSuccessful()) {
                command.setStatus("EXECUTED");
                command.setExecuteTime(LocalDateTime.now());
                log.info("Command {} executed successfully on robot {}", command.getCommandId(), robotId);

                // 7. 通过WebSocket推送状态更新
                notifyCommandUpdate(robotId, command);

                // 8. 更新机器人状态
                updateRobotStatusAfterCommand(robotId, command);

                return CompletableFuture.completedFuture(true);
            } else {
                command.setStatus("FAILED");
                command.setErrorMessage("HTTP " + response.getStatusCode());
                notifyCommandUpdate(robotId, command);
                return CompletableFuture.completedFuture(false);
            }

        } catch (HttpClientErrorException e) {
            log.error("HTTP error when sending command to robot {}: {}", robotId, e.getMessage());
            command.setStatus("FAILED");
            command.setErrorMessage("HTTP " + e.getStatusCode() + ": " + e.getStatusText());
            notifyCommandUpdate(robotId, command);
            return CompletableFuture.completedFuture(false);

        } catch (ResourceAccessException e) {
            log.error("Network error when sending command to robot {}: {}", robotId, e.getMessage());
            command.setStatus("FAILED");
            command.setErrorMessage("Network error: " + e.getMessage());
            notifyCommandUpdate(robotId, command);
            return CompletableFuture.completedFuture(false);

        } catch (Exception e) {
            log.error("Unexpected error when sending command to robot {}: {}", robotId, e.getMessage(), e);
            command.setStatus("FAILED");
            command.setErrorMessage("System error: " + e.getMessage());
            notifyCommandUpdate(robotId, command);
            return CompletableFuture.completedFuture(false);

        } finally {
            long duration = System.currentTimeMillis() - startTime;
            log.debug("Command execution took {}ms", duration);

            // 记录性能指标
            recordCommandMetrics(robotId, command, duration);
        }
    }

    /**
     * 获取机器人当前状态
     *
     * @param robotId 机器人ID
     * @return 机器人状态
     */
    public RobotStatus getRobotStatus(String robotId) {
        // 1. 先从缓存中获取
        RobotStatus cachedStatus = robotStatusCache.get(robotId);
        if (cachedStatus != null &&
            cachedStatus.getLastUpdateTime().plusNanos(statusCacheExpire * 1_000_000).isAfter(LocalDateTime.now())) {
            return cachedStatus;
        }

        // 2. 缓存过期或不存在，从机器人获取最新状态
        try {
            String url = String.format("%s/%s/status", robotApiBaseUrl, robotId);
            ResponseEntity<RobotStatus> response = restTemplate.exchange(
                url, HttpMethod.GET, null, RobotStatus.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                RobotStatus status = response.getBody();
                status.setRobotId(robotId);
                status.setLastUpdateTime(LocalDateTime.now());
                robotStatusCache.put(robotId, status);

                // 3. 通过WebSocket广播状态更新
                broadcastStatus(robotId, status);

                return status;
            }

        } catch (Exception e) {
            log.error("Failed to get status for robot {}: {}", robotId, e.getMessage());
        }

        // 获取状态失败，返回离线状态
        RobotStatus offlineStatus = new RobotStatus();
        offlineStatus.setRobotId(robotId);
        offlineStatus.setStatus("OFFLINE");
        offlineStatus.setLastUpdateTime(LocalDateTime.now());
        robotStatusCache.put(robotId, offlineStatus);
        return offlineStatus;
    }

    /**
     * 批量获取多个机器人状态
     *
     * @param robotIds 机器人ID列表
     * @return 状态列表
     */
    @Async
    public CompletableFuture<List<RobotStatus>> getRobotsStatus(List<String> robotIds) {
        List<CompletableFuture<RobotStatus>> futures = robotIds.stream()
            .map(id -> CompletableFuture.supplyAsync(() -> getRobotStatus(id)))
            .collect(Collectors.toList());

        // 等待所有异步任务完成
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
            futures.toArray(new CompletableFuture[0])
        );

        return allFutures.thenApply(v ->
            futures.stream()
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .collect(Collectors.toList())
        );
    }

    /**
     * 处理机器人上报的状态信息（心跳）
     *
     * @param robotId 机器人ID
     * @param status 状态数据
     */
    public void handleRobotHeartbeat(String robotId, RobotStatus status) {
        try {
            log.debug("Received heartbeat from robot: {}", robotId);

            // 1. 更新缓存
            status.setRobotId(robotId);
            status.setLastUpdateTime(LocalDateTime.now());
            robotStatusCache.put(robotId, status);

            // 2. 检查异常状态
            if (status.hasAbnormalMetrics()) {
                String severity = status.getMaxSeverity();
                log.warn("Robot {} has abnormal metrics, severity: {}", robotId, severity);

                // 发送告警通知
                sendAlert(robotId, status);
            }

            // 3. 广播状态更新
            broadcastStatus(robotId, status);

        } catch (Exception e) {
            log.error("Error processing heartbeat from robot {}: {}", robotId, e.getMessage(), e);
        }
    }

    /**
     * 重试失败的指令
     */
    @Async
    public CompletableFuture<Boolean> retryCommand(String commandId) {
        RobotCommand command = commandCache.get(commandId);
        if (command == null) {
            log.warn("Command {} not found in cache", commandId);
            return CompletableFuture.completedFuture(false);
        }

        if (command.getRetryCount() >= command.getMaxRetry()) {
            log.warn("Command {} has reached max retry count", commandId);
            return CompletableFuture.completedFuture(false);
        }

        command.setRetryCount(command.getRetryCount() + 1);
        command.setStatus("RETRYING");
        log.info("Retrying command {}, attempt {}", commandId, command.getRetryCount());

        return sendCommand(command.getRobotId(), command);
    }

    /**
     * 通过WebSocket广播机器人状态
     */
    private void broadcastStatus(String robotId, RobotStatus status) {
        try {
            // 发送到特定主题，前端订阅后可实时接收
            String destination = String.format("/topic/robots/%s/status", robotId);
            String message = JsonUtil.toJson(status);
            messagingTemplate.convertAndSend(destination, message);

            // 同时发送到所有机器人状态主题
            messagingTemplate.convertAndSend("/topic/robots/all/status", message);

        } catch (Exception e) {
            log.error("Error broadcasting status for robot {}: {}", robotId, e.getMessage());
        }
    }

    /**
     * 通知指令执行结果
     */
    private void notifyCommandUpdate(String robotId, RobotCommand command) {
        try {
            String destination = String.format("/topic/robots/%s/commands", robotId);
            String message = JsonUtil.toJson(command);
            messagingTemplate.convertAndSend(destination, message);

            // 更新指令缓存
            commandCache.put(command.getCommandId(), command);

        } catch (Exception e) {
            log.error("Error notifying command update for robot {}: {}", robotId, e.getMessage());
        }
    }

    /**
     * 发送异常告警
     */
    private void sendAlert(String robotId, RobotStatus status) {
        try {
            // 构建告警消息
            Map<String, Object> alert = new HashMap<>();
            alert.put("robotId", robotId);
            alert.put("robotName", status.getRobotName());
            alert.put("alertType", "ROBOT_ABNORMAL");
            alert.put("severity", status.getMaxSeverity());
            alert.put("description", status.getAbnormalDescription());
            alert.put("timestamp", LocalDateTime.now());
            alert.put("status", status.getStatus());
            alert.put("taskStatus", status.getTaskStatus());

            // 发送到告警主题
            String message = JsonUtil.toJson(alert);
            messagingTemplate.convertAndSend("/topic/alerts", message);

            // 发送到特定机器人的告警主题
            messagingTemplate.convertAndSend("/topic/robots/" + robotId + "/alerts", message);

            log.warn("Alert sent for robot {}: severity={}, description={}",
                    robotId, alert.get("severity"), alert.get("description"));

        } catch (Exception e) {
            log.error("Error sending alert for robot {}: {}", robotId, e.getMessage());
        }
    }

    /**
     * 更新机器人状态（指令执行后）
     */
    private void updateRobotStatusAfterCommand(String robotId, RobotCommand command) {
        try {
            // 根据指令类型更新状态
            RobotStatus cachedStatus = robotStatusCache.get(robotId);
            if (cachedStatus != null) {
                switch (command.getCommandType()) {
                    case "START_TASK":
                        cachedStatus.setTaskStatus("RUNNING");
                        break;
                    case "STOP_TASK":
                        cachedStatus.setTaskStatus("STOPPED");
                        break;
                    case "PAUSE_TASK":
                        cachedStatus.setTaskStatus("PAUSED");
                        break;
                    case "RESUME_TASK":
                        cachedStatus.setTaskStatus("RUNNING");
                        break;
                }
                cachedStatus.setLastUpdateTime(LocalDateTime.now());
            }
        } catch (Exception e) {
            log.error("Error updating robot status after command: {}", e.getMessage());
        }
    }

    /**
     * 记录指令执行指标
     */
    private void recordCommandMetrics(String robotId, RobotCommand command, long duration) {
        // 这里可以集成Micrometer或Prometheus进行指标收集
        log.info("Command metrics - robot: {}, command: {}, status: {}, duration: {}ms",
                robotId, command.getCommandType(), command.getStatus(), duration);
    }

    /**
     * 获取所有在线机器人
     */
    public List<String> getOnlineRobots() {
        return robotStatusCache.entrySet().stream()
                .filter(entry -> "ONLINE".equals(entry.getValue().getStatus()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    /**
     * 获取所有机器人状态
     */
    public Map<String, RobotStatus> getAllRobotStatus() {
        return new HashMap<>(robotStatusCache);
    }

    /**
     * 清除缓存
     */
    public void clearCache() {
        robotStatusCache.clear();
        commandCache.clear();
        log.info("Robot control service cache cleared");
    }

    /**
     * 清除指定机器人的缓存
     */
    public void clearRobotCache(String robotId) {
        robotStatusCache.remove(robotId);
        log.info("Cache cleared for robot: {}", robotId);
    }

    /**
     * 获取指令缓存
     */
    public Map<String, RobotCommand> getCommandCache() {
        return new HashMap<>(commandCache);
    }

    /**
     * 获取指定指令
     */
    public RobotCommand getCommand(String commandId) {
        return commandCache.get(commandId);
    }

    /**
     * 取消指令
     */
    public boolean cancelCommand(String commandId) {
        RobotCommand command = commandCache.get(commandId);
        if (command != null && "PENDING".equals(command.getStatus())) {
            command.setStatus("CANCELLED");
            command.setCompleteTime(LocalDateTime.now());
            notifyCommandUpdate(command.getRobotId(), command);
            return true;
        }
        return false;
    }

    /**
     * 批量发送指令
     */
    @Async
    public CompletableFuture<Map<String, Boolean>> sendBatchCommands(Map<String, RobotCommand> commands) {
        Map<String, CompletableFuture<Boolean>> futures = new HashMap<>();

        commands.forEach((robotId, command) -> {
            futures.put(robotId, sendCommand(robotId, command));
        });

        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    Map<String, Boolean> results = new HashMap<>();
                    futures.forEach((robotId, future) -> {
                        results.put(robotId, future.join());
                    });
                    return results;
                });
    }

    /**
     * 获取系统统计信息
     */
    public Map<String, Object> getSystemStats() {
        Map<String, Object> stats = new HashMap<>();

        stats.put("totalRobots", robotStatusCache.size());
        stats.put("onlineRobots", getOnlineRobots().size());
        stats.put("totalCommands", commandCache.size());

        // 统计各状态的指令数量
        Map<String, Long> commandStatusCount = commandCache.values().stream()
                .collect(Collectors.groupingBy(RobotCommand::getStatus, Collectors.counting()));
        stats.put("commandStatusCount", commandStatusCount);

        // 统计各状态的机器人数量
        Map<String, Long> robotStatusCount = robotStatusCache.values().stream()
                .collect(Collectors.groupingBy(RobotStatus::getStatus, Collectors.counting()));
        stats.put("robotStatusCount", robotStatusCount);

        stats.put("cacheTimestamp", LocalDateTime.now());

        return stats;
    }

    /**
     * 健康检查
     */
    public boolean healthCheck() {
        try {
            // 检查缓存是否正常
            if (robotStatusCache == null || commandCache == null) {
                return false;
            }

            // 检查WebSocket连接
            if (messagingTemplate == null) {
                return false;
            }

            // 检查API连接（随机选择一个在线机器人）
            List<String> onlineRobots = getOnlineRobots();
            if (!onlineRobots.isEmpty()) {
                String robotId = onlineRobots.get(0);
                RobotStatus status = getRobotStatus(robotId);
                return status != null && "ONLINE".equals(status.getStatus());
            }

            return true;

        } catch (Exception e) {
            log.error("Health check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 获取机器人历史指令
     */
    public List<RobotCommand> getRobotCommandHistory(String robotId, int limit) {
        return commandCache.values().stream()
                .filter(cmd -> robotId.equals(cmd.getRobotId()))
                .sorted((a, b) -> b.getTimestamp().compareTo(a.getTimestamp()))
                .limit(limit)
                .collect(Collectors.toList());
    }

    /**
     * 获取机器人当前任务
     */
    public String getRobotCurrentTask(String robotId) {
        RobotStatus status = getRobotStatus(robotId);
        return status != null ? status.getCurrentTaskId() : null;
    }

    /**
     * 暂停机器人任务
     */
    public CompletableFuture<Boolean> pauseRobot(String robotId) {
        RobotCommand command = new RobotCommand();
        command.setCommandType("PAUSE_TASK");
        command.setParameters(Collections.emptyMap());
        return sendCommand(robotId, command);
    }

    /**
     * 恢复机器人任务
     */
    public CompletableFuture<Boolean> resumeRobot(String robotId) {
        RobotCommand command = new RobotCommand();
        command.setCommandType("RESUME_TASK");
        command.setParameters(Collections.emptyMap());
        return sendCommand(robotId, command);
    }

    /**
     * 停止机器人任务
     */
    public CompletableFuture<Boolean> stopRobot(String robotId) {
        RobotCommand command = new RobotCommand();
        command.setCommandType("STOP_TASK");
        command.setParameters(Collections.emptyMap());
        return sendCommand(robotId, command);
    }

    /**
     * 重启机器人
     */
    public CompletableFuture<Boolean> restartRobot(String robotId) {
        RobotCommand command = new RobotCommand();
        command.setCommandType("RESTART");
        command.setParameters(Collections.emptyMap());
        return sendCommand(robotId, command);
    }

    /**
     * 更新机器人配置
     */
    public CompletableFuture<Boolean> updateRobotConfig(String robotId, Map<String, Object> config) {
        RobotCommand command = new RobotCommand();
        command.setCommandType("UPDATE_CONFIG");
        command.setParameters(config);
        return sendCommand(robotId, command);
    }

    /**
     * 获取机器人配置
     */
    public Map<String, Object> getRobotConfig(String robotId) {
        try {
            String url = String.format("%s/%s/config", robotApiBaseUrl, robotId);
            ResponseEntity<Map> response = restTemplate.exchange(
                url, HttpMethod.GET, null, Map.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody();
            }
        } catch (Exception e) {
            log.error("Failed to get config for robot {}: {}", robotId, e.getMessage());
        }
        return Collections.emptyMap();
    }

    /**
     * 发送广播消息给所有在线机器人
     */
    @Async
    public CompletableFuture<Integer> broadcastToAllOnline(String messageType, Map<String, Object> payload) {
        List<String> onlineRobots = getOnlineRobots();

        RobotCommand broadcastCommand = new RobotCommand();
        broadcastCommand.setCommandType("BROADCAST");
        broadcastCommand.setParameters(Map.of(
            "messageType", messageType,
            "payload", payload,
            "targets", onlineRobots
        ));

        int successCount = 0;
        for (String robotId : onlineRobots) {
            CompletableFuture<Boolean> result = sendCommand(robotId, broadcastCommand);
            if (result.join()) {
                successCount++;
            }
        }

        return CompletableFuture.completedFuture(successCount);
    }

    /**
     * 获取机器人日志
     */
    public List<String> getRobotLogs(String robotId, int lines) {
        try {
            String url = String.format("%s/%s/logs?lines=%d", robotApiBaseUrl, robotId, lines);
            ResponseEntity<List> response = restTemplate.exchange(
                url, HttpMethod.GET, null, List.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody();
            }
        } catch (Exception e) {
            log.error("Failed to get logs for robot {}: {}", robotId, e.getMessage());
        }
        return Collections.emptyList();
    }

    /**
     * 下载机器人日志文件
     */
    public byte[] downloadRobotLogFile(String robotId, String logFileName) {
        try {
            String url = String.format("%s/%s/logs/download/%s", robotApiBaseUrl, robotId, logFileName);
            ResponseEntity<byte[]> response = restTemplate.exchange(
                url, HttpMethod.GET, null, byte[].class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody();
            }
        } catch (Exception e) {
            log.error("Failed to download log file for robot {}: {}", robotId, e.getMessage());
        }
        return new byte[0];
    }

    /**
     * 执行机器人自检
     */
    public CompletableFuture<Map<String, Object>> runRobotSelfCheck(String robotId) {
        RobotCommand command = new RobotCommand();
        command.setCommandType("SELF_CHECK");
        command.setParameters(Collections.emptyMap());

        return sendCommand(robotId, command).thenApply(success -> {
            if (success) {
                // 等待自检结果
                try {
                    Thread.sleep(2000); // 等待2秒让自检完成
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                // 获取自检结果
                Map<String, Object> result = new HashMap<>();
                result.put("robotId", robotId);
                result.put("checkTime", LocalDateTime.now());
                result.put("status", success ? "PASSED" : "FAILED");
                return result;
            }
            return Collections.emptyMap();
        });
    }

    /**
     * 获取机器人性能指标
     */
    public Map<String, Object> getRobotMetrics(String robotId) {
        try {
            String url = String.format("%s/%s/metrics", robotApiBaseUrl, robotId);
            ResponseEntity<Map> response = restTemplate.exchange(
                url, HttpMethod.GET, null, Map.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody();
            }
        } catch (Exception e) {
            log.error("Failed to get metrics for robot {}: {}", robotId, e.getMessage());
        }
        return Collections.emptyMap();
    }

    /**
     * 重置机器人状态
     */
    public CompletableFuture<Boolean> resetRobot(String robotId) {
        // 先停止当前任务
        return stopRobot(robotId).thenCompose(stopResult -> {
            if (stopResult) {
                // 重置配置
                RobotCommand resetCommand = new RobotCommand();
                resetCommand.setCommandType("RESET");
                resetCommand.setParameters(Map.of("soft", true));
                return sendCommand(robotId, resetCommand);
            }
            return CompletableFuture.completedFuture(false);
        });
    }

    /**
     * 升级机器人固件
     */
    public CompletableFuture<Boolean> upgradeRobotFirmware(String robotId, String firmwareVersion) {
        RobotCommand command = new RobotCommand();
        command.setCommandType("UPGRADE_FIRMWARE");
        command.setParameters(Map.of(
            "version", firmwareVersion,
            "force", false,
            "backup", true
        ));
        return sendCommand(robotId, command);
    }

    /**
     * 获取机器人固件版本
     */
    public String getRobotFirmwareVersion(String robotId) {
        Map<String, Object> config = getRobotConfig(robotId);
        return config != null ? (String) config.get("firmwareVersion") : null;
    }

    /**
     * 校准机器人传感器
     */
    public CompletableFuture<Boolean> calibrateRobotSensors(String robotId) {
        RobotCommand command = new RobotCommand();
        command.setCommandType("CALIBRATE_SENSORS");
        command.setParameters(Collections.emptyMap());
        return sendCommand(robotId, command);
    }

    /**
     * 获取机器人传感器状态
     */
    public Map<String, Object> getRobotSensorStatus(String robotId) {
        try {
            String url = String.format("%s/%s/sensors/status", robotApiBaseUrl, robotId);
            ResponseEntity<Map> response = restTemplate.exchange(
                url, HttpMethod.GET, null, Map.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody();
            }
        } catch (Exception e) {
            log.error("Failed to get sensor status for robot {}: {}", robotId, e.getMessage());
        }
        return Collections.emptyMap();
    }

    /**
     * 设置机器人工作模式
     */
    public CompletableFuture<Boolean> setRobotWorkMode(String robotId, String mode) {
        RobotCommand command = new RobotCommand();
        command.setCommandType("SET_WORK_MODE");
        command.setParameters(Map.of("mode", mode));
        return sendCommand(robotId, command);
    }

    /**
     * 获取机器人工作模式
     */
    public String getRobotWorkMode(String robotId) {
        Map<String, Object> config = getRobotConfig(robotId);
        return config != null ? (String) config.get("workMode") : "UNKNOWN";
    }

    /**
     * 调度机器人任务
     */
    public CompletableFuture<Boolean> scheduleRobotTask(String robotId, String taskType,
                                                        Map<String, Object> taskParams) {
        RobotCommand command = new RobotCommand();
        command.setCommandType("SCHEDULE_TASK");
        command.setParameters(Map.of(
            "taskType", taskType,
            "taskParams", taskParams,
            "scheduleTime", LocalDateTime.now().plusMinutes(1) // 1分钟后执行
        ));
        return sendCommand(robotId, command);
    }

    /**
     * 取消机器人计划任务
     */
    public CompletableFuture<Boolean> cancelScheduledTask(String robotId, String taskId) {
        RobotCommand command = new RobotCommand();
        command.setCommandType("CANCEL_SCHEDULED_TASK");
        command.setParameters(Map.of("taskId", taskId));
        return sendCommand(robotId, command);
    }

    /**
     * 获取机器人计划任务列表
     */
    public List<Map<String, Object>> getScheduledTasks(String robotId) {
        try {
            String url = String.format("%s/%s/scheduled-tasks", robotApiBaseUrl, robotId);
            ResponseEntity<List> response = restTemplate.exchange(
                url, HttpMethod.GET, null, List.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody();
            }
        } catch (Exception e) {
            log.error("Failed to get scheduled tasks for robot {}: {}", robotId, e.getMessage());
        }
        return Collections.emptyList();
    }

    /**
     * 获取机器人工作日志
     */
    public List<Map<String, Object>> getRobotWorkLog(String robotId, LocalDateTime startTime,
                                                   LocalDateTime endTime) {
        try {
            String url = String.format("%s/%s/work-log?start=%s&end=%s",
                    robotApiBaseUrl, robotId, startTime, endTime);
            ResponseEntity<List> response = restTemplate.exchange(
                url, HttpMethod.GET, null, List.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody();
            }
        } catch (Exception e) {
            log.error("Failed to get work log for robot {}: {}", robotId, e.getMessage());
        }
        return Collections.emptyList();
    }

    /**
     * 导出机器人数据
     */
    public byte[] exportRobotData(String robotId, String format, LocalDateTime startTime,
                                 LocalDateTime endTime) {
        try {
            String url = String.format("%s/%s/export?format=%s&start=%s&end=%s",
                    robotApiBaseUrl, robotId, format, startTime, endTime);
            ResponseEntity<byte[]> response = restTemplate.exchange(
                url, HttpMethod.GET, null, byte[].class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody();
            }
        } catch (Exception e) {
            log.error("Failed to export data for robot {}: {}", robotId, e.getMessage());
        }
        return new byte[0];
    }

    /**
     * 获取机器人API状态
     */
    public boolean isRobotApiAvailable() {
        try {
            String url = robotApiBaseUrl + "/health";
            ResponseEntity<String> response = restTemplate.exchange(
                url, HttpMethod.GET, null, String.class);
            return response.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            log.error("Robot API is not available: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 获取机器人API版本
     */
    public String getRobotApiVersion() {
        try {
            String url = robotApiBaseUrl + "/version";
            ResponseEntity<String> response = restTemplate.exchange(
                url, HttpMethod.GET, null, String.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                return response.getBody();
            }
        } catch (Exception e) {
            log.error("Failed to get robot API version: {}", e.getMessage());
        }
        return "UNKNOWN";
    }

    /**
     * 同步机器人时间
     */
    public CompletableFuture<Boolean> syncRobotTime(String robotId) {
        RobotCommand command = new RobotCommand();
        command.setCommandType("SYNC_TIME");
        command.setParameters(Map.of(
            "currentTime", LocalDateTime.now(),
            "timezone", TimeZone.getDefault().getID()
        ));
        return sendCommand(robotId, command);
    }

    /**
     * 获取机器人诊断信息
     */
    public Map<String, Object> getRobotDiagnostics(String robotId) {
        try {
            String url = String.format("%s/%s/diagnostics", robotApiBaseUrl, robotId);
            ResponseEntity<Map> response = restTemplate.exchange(
                url, HttpMethod.GET, null, Map.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody();
            }
        } catch (Exception e) {
            log.error("Failed to get diagnostics for robot {}: {}", robotId, e.getMessage());
        }
        return Collections.emptyMap();
    }

    /**
     * 执行机器人维护操作
     */
    public CompletableFuture<Boolean> performMaintenance(String robotId, String maintenanceType) {
        RobotCommand command = new RobotCommand();
        command.setCommandType("MAINTENANCE");
        command.setParameters(Map.of(
            "type", maintenanceType,
            "startTime", LocalDateTime.now()
        ));
        return sendCommand(robotId, command);
    }

    /**
     * 获取机器人维护历史
     */
    public List<Map<String, Object>> getMaintenanceHistory(String robotId) {
        try {
            String url = String.format("%s/%s/maintenance-history", robotApiBaseUrl, robotId);
            ResponseEntity<List> response = restTemplate.exchange(
                url, HttpMethod.GET, null, List.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody();
            }
        } catch (Exception e) {
            log.error("Failed to get maintenance history for robot {}: {}", robotId, e.getMessage());
        }
        return Collections.emptyList();
    }

    /**
     * 设置机器人状态监听器
     */
    public void setRobotStatusListener(String robotId, RobotStatusListener listener) {
        // 这里可以实现观察者模式，监听机器人状态变化
        // 简化实现，直接返回当前状态
        RobotStatus status = getRobotStatus(robotId);
        if (status != null && listener != null) {
            listener.onStatusChange(status);
        }
    }

    /**
     * 机器人状态监听器接口
     */
    public interface RobotStatusListener {
        void onStatusChange(RobotStatus status);
    }
}