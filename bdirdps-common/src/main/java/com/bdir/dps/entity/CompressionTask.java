package com.bdir.dps.entity;

import java.time.LocalDateTime;

/**
 * 数据压缩任务实体类
 * 记录数据压缩任务的详细信息
 *
 * @author BDIRDPSys开发团队
 * @since 2026-01-16
 */
public class CompressionTask {

    private String taskId;
    private String status;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private int totalFiles;
    private int compressedFiles;
    private long originalSize;
    private long compressedSize;
    private String compressionAlgorithm;
    private String errorMessage;

    public CompressionTask() {
        this.taskId = java.util.UUID.randomUUID().toString();
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
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

    public int getTotalFiles() {
        return totalFiles;
    }

    public void setTotalFiles(int totalFiles) {
        this.totalFiles = totalFiles;
    }

    public int getCompressedFiles() {
        return compressedFiles;
    }

    public void setCompressedFiles(int compressedFiles) {
        this.compressedFiles = compressedFiles;
    }

    public long getOriginalSize() {
        return originalSize;
    }

    public void setOriginalSize(long originalSize) {
        this.originalSize = originalSize;
    }

    public long getCompressedSize() {
        return compressedSize;
    }

    public void setCompressedSize(long compressedSize) {
        this.compressedSize = compressedSize;
    }

    public String getCompressionAlgorithm() {
        return compressionAlgorithm;
    }

    public void setCompressionAlgorithm(String compressionAlgorithm) {
        this.compressionAlgorithm = compressionAlgorithm;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * 计算压缩率
     */
    public double getCompressionRatio() {
        if (originalSize > 0) {
            return (1.0 - (double) compressedSize / originalSize) * 100;
        }
        return 0.0;
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
}