package com.bdir.dps.service;

import com.bdir.dps.common.Result;
import com.bdir.dps.entity.CompressionTask;
import com.bdir.dps.utils.CompressionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 数据压缩服务
 * 负责压缩历史数据以节省存储空间
 *
 * @author BDIRDPSys开发团队
 * @since 2026-01-16
 */
@Service
public class DataCompressionService {

    private static final Logger logger = LoggerFactory.getLogger(DataCompressionService.class);

    @Value("${data.compression.enabled:true}")
    private boolean compressionEnabled;

    @Value("${data.compression.algorithm:SNAPPY}")
    private String compressionAlgorithm;

    @Value("${data.compression.threshold.days:30}")
    private int compressionThresholdDays;

    @Autowired
    private CompressionUtil compressionUtil;

    /**
     * 压缩历史数据
     * 异步处理超过阈值的历史数据
     */
    @Async("compressionTaskExecutor")
    public CompletableFuture<Result<String>> compressHistoricalData(LocalDateTime beforeDate) {
        try {
            if (!compressionEnabled) {
                return CompletableFuture.completedFuture(Result.error("数据压缩功能已禁用"));
            }

            logger.info("开始压缩 {} 之前的历史数据", beforeDate);

            // 1. 查询需要压缩的数据文件
            List<File< dataFiles = findDataFilesBeforeDate(beforeDate);

            if (dataFiles.isEmpty()) {
                return CompletableFuture.completedFuture(Result.success("没有需要压缩的数据"));
            }

            logger.info("找到 {} 个需要压缩的数据文件", dataFiles.size());

            // 2. 压缩数据文件
            CompressionTask task = new CompressionTask();
            task.setStartTime(LocalDateTime.now());
            task.setTotalFiles(dataFiles.size());
            task.setStatus("RUNNING");

            long originalSize = 0;
            long compressedSize = 0;
            int successCount = 0;

            for (File file : dataFiles) {
                try {
                    // 压缩单个文件
                    File compressedFile = compressionUtil.compressFile(file, compressionAlgorithm);

                    originalSize += file.length();
                    compressedSize += compressedFile.length();
                    successCount++;

                    // 删除原始文件（可选）
                    if (compressionUtil.deleteOriginalAfterCompression()) {
                        file.delete();
                    }

                } catch (Exception e) {
                    logger.error("压缩文件 {} 失败", file.getName(), e);
                }
            }

            // 3. 更新任务状态
            task.setEndTime(LocalDateTime.now());
            task.setCompressedFiles(successCount);
            task.setOriginalSize(originalSize);
            task.setCompressedSize(compressedSize);
            task.setStatus("COMPLETED");

            // 计算压缩率
            double compressionRatio = originalSize > 0 ?
                (1.0 - (double) compressedSize / originalSize) * 100 : 0;

            logger.info("数据压缩完成，压缩文件数：{}，压缩率：{:.2f}%",
                successCount, compressionRatio);

            return CompletableFuture.completedFuture(Result.success(
                String.format("数据压缩完成，压缩率：%.2f%%，节省空间：%.2f MB",
                    compressionRatio, (originalSize - compressedSize) / 1024.0 / 1024.0)
            ));

        } catch (Exception e) {
            logger.error("数据压缩失败", e);
            return CompletableFuture.completedFuture(Result.error("数据压缩失败：" + e.getMessage()));
        }
    }

    /**
     * 解压缩数据文件
     */
    @Async("compressionTaskExecutor")
    public CompletableFuture<Result<String>> decompressDataFile(String compressedFilePath) {
        try {
            File compressedFile = new File(compressedFilePath);
            if (!compressedFile.exists()) {
                return CompletableFuture.completedFuture(Result.error("压缩文件不存在"));
            }

            logger.info("开始解压缩文件：{}", compressedFilePath);

            // 解压缩文件
            File decompressedFile = compressionUtil.decompressFile(compressedFile);

            logger.info("文件解压缩完成：{} -\u003e {}",
                compressedFile.getName(), decompressedFile.getName());

            return CompletableFuture.completedFuture(Result.success(
                String.format("文件解压缩完成：%s", decompressedFile.getAbsolutePath())
            ));

        } catch (Exception e) {
            logger.error("文件解压缩失败", e);
            return CompletableFuture.completedFuture(Result.error("文件解压缩失败：" + e.getMessage()));
        }
    }

    /**
     * 查询压缩任务状态
     */
    public Result<CompressionTask> getCompressionTask(String taskId) {
        try {
            CompressionTask task = compressionUtil.getTaskById(taskId);
            if (task == null) {
                return Result.error("任务不存在");
            }
            return Result.success(task);
        } catch (Exception e) {
            logger.error("查询压缩任务失败", e);
            return Result.error("查询失败：" + e.getMessage());
        }
    }

    /**
     * 获取压缩统计信息
     */
    public Result<CompressionStatistics> getCompressionStatistics() {
        try {
            CompressionStatistics stats = compressionUtil.getCompressionStatistics();
            return Result.success(stats);
        } catch (Exception e) {
            logger.error("获取压缩统计信息失败", e);
            return Result.error("获取统计信息失败：" + e.getMessage());
        }
    }

    /**
     * 查找指定日期之前的数据文件
     */
    private List<File> findDataFilesBeforeDate(LocalDateTime beforeDate) {
        // 这里应该实现具体的文件查找逻辑
        // 根据实际需求从HDFS或本地文件系统中查找数据文件
        return compressionUtil.findDataFilesBeforeDate(beforeDate);
    }

    /**
     * 内部类：压缩统计信息
     */
    public static class CompressionStatistics {
        private long totalCompressedFiles;
        private long totalOriginalSize;
        private long totalCompressedSize;
        private double averageCompressionRatio;
        private LocalDateTime lastCompressionTime;

        // getters and setters
        public long getTotalCompressedFiles() {
            return totalCompressedFiles;
        }

        public void setTotalCompressedFiles(long totalCompressedFiles) {
            this.totalCompressedFiles = totalCompressedFiles;
        }

        public long getTotalOriginalSize() {
            return totalOriginalSize;
        }

        public void setTotalOriginalSize(long totalOriginalSize) {
            this.totalOriginalSize = totalOriginalSize;
        }

        public long getTotalCompressedSize() {
            return totalCompressedSize;
        }

        public void setTotalCompressedSize(long totalCompressedSize) {
            this.totalCompressedSize = totalCompressedSize;
        }

        public double getAverageCompressionRatio() {
            return averageCompressionRatio;
        }

        public void setAverageCompressionRatio(double averageCompressionRatio) {
            this.averageCompressionRatio = averageCompressionRatio;
        }

        public LocalDateTime getLastCompressionTime() {
            return lastCompressionTime;
        }

        public void setLastCompressionTime(LocalDateTime lastCompressionTime) {
            this.lastCompressionTime = lastCompressionTime;
        }
    }
}