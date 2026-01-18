package com.bdir.dps.entity;

import java.time.LocalDateTime;

/**
 * 数据备份任务实体类
 * 记录数据备份任务的详细信息
 *
 * @author BDIRDPSys开发团队
 * @since 2026-01-16
 */
public class BackupTask {

    private String taskId;
    private String taskType; // FULL_BACKUP, INCREMENTAL_BACKUP
    private String status; // RUNNING, COMPLETED, FAILED
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private LocalDateTime lastBackupTime;
    private String hiveBackupPath;
    private String mysqlBackupPath;
    private String configBackupPath;
    private String manifestPath;
    private String errorMessage;
    private long backupSize;
    private int fileCount;

    public BackupTask() {
        this.taskId = java.util.UUID.randomUUID().toString();
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public LocalDateTime getLastBackupTime() {
        return lastBackupTime;
    }

    public void setLastBackupTime(LocalDateTime lastBackupTime) {
        this.lastBackupTime = lastBackupTime;
    }

    public String getHiveBackupPath() {
        return hiveBackupPath;
    }

    public void setHiveBackupPath(String hiveBackupPath) {
        this.hiveBackupPath = hiveBackupPath;
    }

    public String getMysqlBackupPath() {
        return mysqlBackupPath;
    }

    public void setMysqlBackupPath(String mysqlBackupPath) {
        this.mysqlBackupPath = mysqlBackupPath;
    }

    public String getConfigBackupPath() {
        return configBackupPath;
    }

    public void setConfigBackupPath(String configBackupPath) {
        this.configBackupPath = configBackupPath;
    }

    public String getManifestPath() {
        return manifestPath;
    }

    public void setManifestPath(String manifestPath) {
        this.manifestPath = manifestPath;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public long getBackupSize() {
        return backupSize;
    }

    public void setBackupSize(long backupSize) {
        this.backupSize = backupSize;
    }

    public int getFileCount() {
        return fileCount;
    }

    public void setFileCount(int fileCount) {
        this.fileCount = fileCount;
    }

    /**
     * 计算耗时（秒）
     */
    public long getDurationInSeconds() {
        if (startTime != null && endTime != null) {
            return java.time.Duration.between(startTime, endTime).getSeconds();
        }
        return 0;
    }

    /**
     * 获取备份大小（MB）
     */
    public double getBackupSizeInMB() {
        return backupSize / 1024.0 / 1024.0;
    }

    /**
     * 判断是否成功
     */
    public boolean isSuccess() {
        return "COMPLETED".equals(status);
    }

    /**
     * 判断是否失败
     */
    public boolean isFailed() {
        return "FAILED".equals(status);
    }

    /**
     * 判断是否运行中
     */
    public boolean isRunning() {
        return "RUNNING".equals(status);
    }

    @Override
    public String toString() {
        return "BackupTask{" +
                "taskId='" + taskId + '\'' +
                ", taskType='" + taskType + '\'' +
                ", status='" + status + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", backupSize=" + backupSize +
                ", fileCount=" + fileCount +
                '}';
    }
}