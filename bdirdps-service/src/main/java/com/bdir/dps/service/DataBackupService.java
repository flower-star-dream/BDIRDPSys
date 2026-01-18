package com.bdir.dps.service;

import com.bdir.dps.common.Result;
import com.bdir.dps.entity.BackupTask;
import com.bdir.dps.utils.BackupUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 数据备份服务
 * 负责数据备份和恢复功能
 *
 * @author BDIRDPSys开发团队
 * @since 2026-01-16
 */
@Service
public class DataBackupService {

    private static final Logger logger = LoggerFactory.getLogger(DataBackupService.class);

    @Value("${data.backup.enabled:true}")
    private boolean backupEnabled;

    @Value("${data.backup.location:/backup/data}")
    private String backupLocation;

    @Value("${data.backup.retention.days:30}")
    private int retentionDays;

    @Autowired
    private BackupUtil backupUtil;

    /**
     * 执行全量数据备份
     * 备份所有传感器数据和元数据
     */
    @Async("backupTaskExecutor")
    public CompletableFuture<Result<String>> performFullBackup() {
        try {
            if (!backupEnabled) {
                return CompletableFuture.completedFuture(Result.error("数据备份功能已禁用"));
            }

            logger.info("开始执行全量数据备份");

            // 创建备份任务
            BackupTask task = new BackupTask();
            task.setTaskId(java.util.UUID.randomUUID().toString());
            task.setTaskType("FULL_BACKUP");
            task.setStatus("RUNNING");
            task.setStartTime(LocalDateTime.now());

            // 1. 备份Hive数据
            logger.info("开始备份Hive数据...");
            Result<String> hiveBackupResult = backupHiveData();
            if (!hiveBackupResult.isSuccess()) {
                throw new RuntimeException("Hive数据备份失败: " + hiveBackupResult.getMessage());
            }
            task.setHiveBackupPath(hiveBackupResult.getData());

            // 2. 备份MySQL数据
            logger.info("开始备份MySQL数据...");
            Result<String> mysqlBackupResult = backupMySQLData();
            if (!mysqlBackupResult.isSuccess()) {
                throw new RuntimeException("MySQL数据备份失败: " + mysqlBackupResult.getMessage());
            }
            task.setMysqlBackupPath(mysqlBackupResult.getData());

            // 3. 备份配置文件
            logger.info("开始备份配置文件...");
            Result<String> configBackupResult = backupConfigurationFiles();
            if (!configBackupResult.isSuccess()) {
                throw new RuntimeException("配置文件备份失败: " + configBackupResult.getMessage());
            }
            task.setConfigBackupPath(configBackupResult.getData());

            // 4. 生成备份清单
            logger.info("生成备份清单...");
            String manifestPath = backupUtil.createBackupManifest(task);
            task.setManifestPath(manifestPath);

            // 更新任务状态
            task.setStatus("COMPLETED");
            task.setEndTime(LocalDateTime.now());

            logger.info("全量数据备份完成，备份清单：{}", manifestPath);

            return CompletableFuture.completedFuture(Result.success(
                String.format("全量数据备份完成，备份路径：%s", manifestPath)
            ));

        } catch (Exception e) {
            logger.error("全量数据备份失败", e);
            return CompletableFuture.completedFuture(Result.error("全量数据备份失败：" + e.getMessage()));
        }
    }

    /**
     * 执行增量数据备份
     * 只备份自上次备份以来的变化数据
     */
    @Async("backupTaskExecutor")
    public CompletableFuture<Result<String>> performIncrementalBackup(LocalDateTime lastBackupTime) {
        try {
            if (!backupEnabled) {
                return CompletableFuture.completedFuture(Result.error("数据备份功能已禁用"));
            }

            logger.info("开始执行增量数据备份，上次备份时间：{}", lastBackupTime);

            // 创建备份任务
            BackupTask task = new BackupTask();
            task.setTaskId(java.util.UUID.randomUUID().toString());
            task.setTaskType("INCREMENTAL_BACKUP");
            task.setStatus("RUNNING");
            task.setStartTime(LocalDateTime.now());
            task.setLastBackupTime(lastBackupTime);

            // 1. 备份增量Hive数据
            logger.info("开始备份增量Hive数据...");
            Result<String> hiveIncrementalResult = backupIncrementalHiveData(lastBackupTime);
            if (!hiveIncrementalResult.isSuccess()) {
                throw new RuntimeException("增量Hive数据备份失败: " + hiveIncrementalResult.getMessage());
            }
            task.setHiveBackupPath(hiveIncrementalResult.getData());

            // 2. 备份增量MySQL数据
            logger.info("开始备份增量MySQL数据...");
            Result<String> mysqlIncrementalResult = backupIncrementalMySQLData(lastBackupTime);
            if (!mysqlIncrementalResult.isSuccess()) {
                throw new RuntimeException("增量MySQL数据备份失败: " + mysqlIncrementalResult.getMessage());
            }
            task.setMysqlBackupPath(mysqlIncrementalResult.getData());

            // 更新任务状态
            task.setStatus("COMPLETED");
            task.setEndTime(LocalDateTime.now());

            logger.info("增量数据备份完成");

            return CompletableFuture.completedFuture(Result.success("增量数据备份完成"));

        } catch (Exception e) {
            logger.error("增量数据备份失败", e);
            return CompletableFuture.completedFuture(Result.error("增量数据备份失败：" + e.getMessage()));
        }
    }

    /**
     * 恢复数据
     */
    @Async("backupTaskExecutor")
    public CompletableFuture<Result<String>> restoreData(String backupManifestPath, RestoreOptions options) {
        try {
            logger.info("开始恢复数据，备份清单：{}", backupManifestPath);

            // 验证备份清单
            File manifestFile = new File(backupManifestPath);
            if (!manifestFile.exists()) {
                return CompletableFuture.completedFuture(Result.error("备份清单不存在"));
            }

            // 读取备份任务信息
            BackupTask task = backupUtil.readBackupManifest(backupManifestPath);
            if (task == null) {
                return CompletableFuture.completedFuture(Result.error("无效的备份清单"));
            }

            // 1. 恢复前检查
            if (options.isDryRun()) {
                logger.info("执行恢复前检查...");
                Result<Boolean> checkResult = performPreRestoreCheck(task);
                if (!checkResult.isSuccess()) {
                    return CompletableFuture.completedFuture(Result.error("恢复前检查失败：" + checkResult.getMessage()));
                }
                return CompletableFuture.completedFuture(Result.success("恢复前检查通过"));
            }

            // 2. 执行恢复
            logger.info("开始执行数据恢复...");

            // 恢复Hive数据
            if (task.getHiveBackupPath() != null) {
                logger.info("开始恢复Hive数据...");
                Result<String> hiveRestoreResult = restoreHiveData(task.getHiveBackupPath());
                if (!hiveRestoreResult.isSuccess()) {
                    throw new RuntimeException("Hive数据恢复失败: " + hiveRestoreResult.getMessage());
                }
            }

            // 恢复MySQL数据
            if (task.getMysqlBackupPath() != null) {
                logger.info("开始恢复MySQL数据...");
                Result<String> mysqlRestoreResult = restoreMySQLData(task.getMysqlBackupPath());
                if (!mysqlRestoreResult.isSuccess()) {
                    throw new RuntimeException("MySQL数据恢复失败: " + mysqlRestoreResult.getMessage());
                }
            }

            // 恢复配置文件
            if (task.getConfigBackupPath() != null) {
                logger.info("开始恢复配置文件...");
                Result<String> configRestoreResult = restoreConfigurationFiles(task.getConfigBackupPath());
                if (!configRestoreResult.isSuccess()) {
                    throw new RuntimeException("配置文件恢复失败: " + configRestoreResult.getMessage());
                }
            }

            logger.info("数据恢复完成");
            return CompletableFuture.completedFuture(Result.success("数据恢复完成"));

        } catch (Exception e) {
            logger.error("数据恢复失败", e);
            return CompletableFuture.completedFuture(Result.error("数据恢复失败：" + e.getMessage()));
        }
    }

    /**
     * 定时清理过期备份
     */
    @Scheduled(cron = "${data.backup.cleanup.cron:0 0 2 * * ?}")
    public void cleanupExpiredBackups() {
        try {
            logger.info("开始清理过期备份文件，保留天数：{}", retentionDays);

            LocalDateTime expirationTime = LocalDateTime.now().minusDays(retentionDays);
            List<File> expiredBackups = backupUtil.findExpiredBackups(backupLocation, expirationTime);

            if (expiredBackups.isEmpty()) {
                logger.info("没有过期的备份文件需要清理");
                return;
            }

            logger.info("找到 {} 个过期备份文件", expiredBackups.size());

            int deletedCount = 0;
            long freedSpace = 0;

            for (File backup : expiredBackups) {
                long fileSize = backup.length();
                if (backup.delete()) {
                    deletedCount++;
                    freedSpace += fileSize;
                    logger.debug("删除过期备份文件：{}", backup.getName());
                }
            }

            logger.info("清理完成，删除 {} 个文件，释放空间 {} MB",
                deletedCount, freedSpace / 1024.0 / 1024.0);

        } catch (Exception e) {
            logger.error("清理过期备份失败", e);
        }
    }

    /**
     * 查询备份任务列表
     */
    public Result<List<BackupTask>> getBackupTaskList(int page, int size) {
        try {
            List<BackupTask> tasks = backupUtil.getBackupTasks(page, size);
            return Result.success(tasks);
        } catch (Exception e) {
            logger.error("查询备份任务失败", e);
            return Result.error("查询备份任务失败：" + e.getMessage());
        }
    }

    /**
     * 获取备份统计信息
     */
    public Result<BackupStatistics> getBackupStatistics() {
        try {
            BackupStatistics stats = backupUtil.getBackupStatistics();
            return Result.success(stats);
        } catch (Exception e) {
            logger.error("获取备份统计信息失败", e);
            return Result.error("获取备份统计信息失败：" + e.getMessage());
        }
    }

    // 私有方法

    private Result<String> backupHiveData() {
        // 实现Hive数据备份逻辑
        return Result.success("/backup/hive/" + System.currentTimeMillis());
    }

    private Result<String> backupMySQLData() {
        // 实现MySQL数据备份逻辑
        return Result.success("/backup/mysql/" + System.currentTimeMillis());
    }

    private Result<String> backupConfigurationFiles() {
        // 实现配置文件备份逻辑
        return Result.success("/backup/config/" + System.currentTimeMillis());
    }

    private Result<String> backupIncrementalHiveData(LocalDateTime lastBackupTime) {
        // 实现增量Hive数据备份逻辑
        return Result.success("/backup/hive/incremental/" + System.currentTimeMillis());
    }

    private Result<String> backupIncrementalMySQLData(LocalDateTime lastBackupTime) {
        // 实现增量MySQL数据备份逻辑
        return Result.success("/backup/mysql/incremental/" + System.currentTimeMillis());
    }

    private Result<Boolean> performPreRestoreCheck(BackupTask task) {
        // 实现恢复前检查逻辑
        return Result.success(true);
    }

    private Result<String> restoreHiveData(String backupPath) {
        // 实现Hive数据恢复逻辑
        return Result.success("Hive数据恢复完成");
    }

    private Result<String> restoreMySQLData(String backupPath) {
        // 实现MySQL数据恢复逻辑
        return Result.success("MySQL数据恢复完成");
    }

    private Result<String> restoreConfigurationFiles(String backupPath) {
        // 实现配置文件恢复逻辑
        return Result.success("配置文件恢复完成");
    }

    /**
     * 恢复选项
     */
    public static class RestoreOptions {
        private boolean dryRun = false;
        private boolean overwriteExisting = false;
        private boolean restoreConfig = true;
        private boolean restoreData = true;

        public boolean isDryRun() {
            return dryRun;
        }

        public void setDryRun(boolean dryRun) {
            this.dryRun = dryRun;
        }

        public boolean isOverwriteExisting() {
            return overwriteExisting;
        }

        public void setOverwriteExisting(boolean overwriteExisting) {
            this.overwriteExisting = overwriteExisting;
        }

        public boolean isRestoreConfig() {
            return restoreConfig;
        }

        public void setRestoreConfig(boolean restoreConfig) {
            this.restoreConfig = restoreConfig;
        }

        public boolean isRestoreData() {
            return restoreData;
        }

        public void setRestoreData(boolean restoreData) {
            this.restoreData = restoreData;
        }
    }

    /**
     * 备份统计信息
     */
    public static class BackupStatistics {
        private long totalBackupSize;
        private int totalBackupCount;
        private int fullBackupCount;
        private int incrementalBackupCount;
        private LocalDateTime lastBackupTime;
        private long averageBackupSize;

        // getters and setters
        public long getTotalBackupSize() {
            return totalBackupSize;
        }

        public void setTotalBackupSize(long totalBackupSize) {
            this.totalBackupSize = totalBackupSize;
        }

        public int getTotalBackupCount() {
            return totalBackupCount;
        }

        public void setTotalBackupCount(int totalBackupCount) {
            this.totalBackupCount = totalBackupCount;
        }

        public int getFullBackupCount() {
            return fullBackupCount;
        }

        public void setFullBackupCount(int fullBackupCount) {
            this.fullBackupCount = fullBackupCount;
        }

        public int getIncrementalBackupCount() {
            return incrementalBackupCount;
        }

        public void setIncrementalBackupCount(int incrementalBackupCount) {
            this.incrementalBackupCount = incrementalBackupCount;
        }

        public LocalDateTime getLastBackupTime() {
            return lastBackupTime;
        }

        public void setLastBackupTime(LocalDateTime lastBackupTime) {
            this.lastBackupTime = lastBackupTime;
        }

        public long getAverageBackupSize() {
            return averageBackupSize;
        }

        public void setAverageBackupSize(long averageBackupSize) {
            this.averageBackupSize = averageBackupSize;
        }
    }
}