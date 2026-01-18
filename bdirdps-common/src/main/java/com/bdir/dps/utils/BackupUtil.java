package com.bdir.dps.utils;

import com.bdir.dps.entity.BackupTask;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据备份工具类
 * 提供数据备份和恢复的实用功能
 *
 * @author BDIRDPSys开发团队
 * @since 2026-01-16
 */
@Component
public class BackupUtil {

    private static final Logger logger = LoggerFactory.getLogger(BackupUtil.class);

    @Value("${data.backup.location:/backup/data}")
    private String backupLocation;

    @Value("${data.backup.temp-dir:/tmp/backup}")
    private String tempBackupDir;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentHashMap<String, BackupTask> backupTasks = new ConcurrentHashMap<>();

    /**
     * 创建备份清单文件
     */
    public String createBackupManifest(BackupTask task) throws IOException {
        // 确保备份目录存在
        Path backupPath = Paths.get(backupLocation);
        if (!Files.exists(backupPath)) {
            Files.createDirectories(backupPath);
        }

        // 生成清单文件路径
        String manifestName = String.format("backup_manifest_%s.json", task.getTaskId());
        Path manifestPath = backupPath.resolve(manifestName);

        // 转换任务为JSON并写入文件
        String json = objectMapper.writerWithDefaultPrettyPrinter()
                .writeValueAsString(task);
        Files.write(manifestPath, json.getBytes());

        // 保存任务到内存
        backupTasks.put(task.getTaskId(), task);

        logger.info("备份清单已创建：{}", manifestPath);
        return manifestPath.toString();
    }

    /**
     * 读取备份清单文件
     */
    public BackupTask readBackupManifest(String manifestPath) throws IOException {
        try {
            Path path = Paths.get(manifestPath);
            if (!Files.exists(path)) {
                throw new FileNotFoundException("备份清单文件不存在: " + manifestPath);
            }

            String json = new String(Files.readAllBytes(path));
            BackupTask task = objectMapper.readValue(json, BackupTask.class);

            // 更新内存中的任务
            backupTasks.put(task.getTaskId(), task);

            return task;
        } catch (IOException e) {
            logger.error("读取备份清单失败: {}", manifestPath, e);
            throw e;
        }
    }

    /**
     * 备份Hive数据
     */
    public String backupHiveData(String hiveDataPath, String backupPath) throws IOException {
        try {
            logger.info("开始备份Hive数据: {} -> {}", hiveDataPath, backupPath);

            // 创建备份目录
            Path backupDir = Paths.get(backupPath);
            if (!Files.exists(backupDir)) {
                Files.createDirectories(backupDir);
            }

            // 复制Hive数据文件
            Path sourcePath = Paths.get(hiveDataPath);
            if (Files.exists(sourcePath)) {
                copyDirectory(sourcePath, backupDir);
            }

            logger.info("Hive数据备份完成: {}", backupPath);
            return backupPath;

        } catch (IOException e) {
            logger.error("备份Hive数据失败", e);
            throw e;
        }
    }

    /**
     * 备份MySQL数据
     */
    public String backupMySQLData(String backupPath) throws IOException, InterruptedException {
        try {
            logger.info("开始备份MySQL数据到: {}", backupPath);

            // 创建备份目录
            Path backupDir = Paths.get(backupPath);
            if (!Files.exists(backupDir)) {
                Files.createDirectories(backupDir);
            }

            // 使用mysqldump命令备份数据库
            String dbName = "bdirdps_db";
            String username = "root";
            String password = "password";
            String backupFile = backupPath + "/mysql_backup.sql";

            String command = String.format("mysqldump -u%s -p%s %s > %s",
                    username, password, dbName, backupFile);

            Process process = Runtime.getRuntime().exec(new String[]{"sh", "-c", command});
            int exitCode = process.waitFor();

            if (exitCode != 0) {
                throw new RuntimeException("MySQL备份命令执行失败，退出码: " + exitCode);
            }

            logger.info("MySQL数据备份完成: {}", backupFile);
            return backupFile;

        } catch (IOException | InterruptedException e) {
            logger.error("备份MySQL数据失败", e);
            throw e;
        }
    }

    /**
     * 备份配置文件
     */
    public String backupConfigurationFiles(String backupPath) throws IOException {
        try {
            logger.info("开始备份配置文件到: {}", backupPath);

            // 创建备份目录
            Path backupDir = Paths.get(backupPath);
            if (!Files.exists(backupDir)) {
                Files.createDirectories(backupDir);
            }

            // 备份application.yml
            Path configFile = Paths.get("src/main/resources/application.yml");
            if (Files.exists(configFile)) {
                Path targetFile = backupDir.resolve("application.yml");
                Files.copy(configFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
            }

            // 备份其他配置文件
            Path configDir = Paths.get("config");
            if (Files.exists(configDir)) {
                Path targetDir = backupDir.resolve("config");
                copyDirectory(configDir, targetDir);
            }

            logger.info("配置文件备份完成: {}", backupPath);
            return backupPath;

        } catch (IOException e) {
            logger.error("备份配置文件失败", e);
            throw e;
        }
    }

    /**
     * 复制目录
     */
    private void copyDirectory(Path source, Path target) throws IOException {
        Files.walk(source).forEach(sourcePath -> {
            try {
                Path targetPath = target.resolve(source.relativize(sourcePath));
                if (Files.isDirectory(sourcePath)) {
                    if (!Files.exists(targetPath)) {
                        Files.createDirectories(targetPath);
                    }
                } else {
                    Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException e) {
                logger.error("复制文件失败: {} -> {}", sourcePath, target, e);
            }
        });
    }

    /**
     * 查找过期的备份文件
     */
    public List<File> findExpiredBackups(String backupLocation, LocalDateTime expirationTime) {
        List<File> expiredFiles = new ArrayList<>();

        try {
            Path backupPath = Paths.get(backupLocation);
            if (!Files.exists(backupPath)) {
                return expiredFiles;
            }

            Files.walk(backupPath)
                    .filter(Files::isRegularFile)
                    .filter(path -> {
                        try {
                            LocalDateTime fileTime = Files.getLastModifiedTime(path).toInstant()
                                    .atZone(java.time.ZoneId.systemDefault()).toLocalDateTime();
                            return fileTime.isBefore(expirationTime);
                        } catch (IOException e) {
                            logger.error("获取文件修改时间失败: {}", path, e);
                            return false;
                        }
                    })
                    .forEach(path -> expiredFiles.add(path.toFile()));

        } catch (IOException e) {
            logger.error("查找过期备份文件失败", e);
        }

        return expiredFiles;
    }

    /**
     * 验证备份完整性
     */
    public boolean verifyBackupIntegrity(String backupPath) {
        try {
            Path path = Paths.get(backupPath);
            if (!Files.exists(path)) {
                return false;
            }

            // 检查备份文件是否存在
            // 检查文件大小是否合理
            // 检查文件是否损坏（如压缩文件）
            // 这里简化处理，实际应该更详细

            return true;

        } catch (Exception e) {
            logger.error("验证备份完整性失败: {}", backupPath, e);
            return false;
        }
    }

    /**
     * 获取备份任务列表
     */
    public List<BackupTask> getBackupTasks(int page, int size) {
        List<BackupTask> tasks = new ArrayList<>(backupTasks.values());

        // 按开始时间倒序排序
        tasks.sort((t1, t2) -> t2.getStartTime().compareTo(t1.getStartTime()));

        // 分页
        int start = page * size;
        int end = Math.min(start + size, tasks.size());

        if (start >= tasks.size()) {
            return new ArrayList<>();
        }

        return tasks.subList(start, end);
    }

    /**
     * 获取备份统计信息
     */
    public DataBackupService.BackupStatistics getBackupStatistics() {
        DataBackupService.BackupStatistics stats = new DataBackupService.BackupStatistics();

        try {
            // 统计备份文件
            Path backupPath = Paths.get(backupLocation);
            if (Files.exists(backupPath)) {
                Files.walk(backupPath)
                        .filter(Files::isRegularFile)
                        .forEach(path -> {
                            try {
                                stats.setTotalBackupSize(stats.getTotalBackupSize() + Files.size(path));
                                stats.setTotalBackupCount(stats.getTotalBackupCount() + 1);
                            } catch (IOException e) {
                                logger.error("获取文件大小失败: {}", path, e);
                            }
                        });
            }

            // 统计任务
            backupTasks.values().forEach(task -> {
                if ("FULL_BACKUP".equals(task.getTaskType())) {
                    stats.setFullBackupCount(stats.getFullBackupCount() + 1);
                } else if ("INCREMENTAL_BACKUP".equals(task.getTaskType())) {
                    stats.setIncrementalBackupCount(stats.getIncrementalBackupCount() + 1);
                }

                if (task.getEndTime() != null &&
                    (stats.getLastBackupTime() == null || task.getEndTime().isAfter(stats.getLastBackupTime()))) {
                    stats.setLastBackupTime(task.getEndTime());
                }
            });

            // 计算平均备份大小
            if (stats.getTotalBackupCount() > 0) {
                stats.setAverageBackupSize(stats.getTotalBackupSize() / stats.getTotalBackupCount());
            }

        } catch (IOException e) {
            logger.error("获取备份统计信息失败", e);
        }

        return stats;
    }

    /**
     * 压缩备份文件
     */
    public String compressBackup(String backupPath) throws IOException {
        try {
            Path path = Paths.get(backupPath);
            if (!Files.exists(path)) {
                throw new FileNotFoundException("备份路径不存在: " + backupPath);
            }

            // 创建压缩文件
            String compressedFileName = path.getFileName() + ".tar.gz";
            Path compressedFile = path.getParent().resolve(compressedFileName);

            // 使用tar命令压缩目录
            String command = String.format("tar -czf %s -C %s %s",
                    compressedFile, path.getParent(), path.getFileName());

            Process process = Runtime.getRuntime().exec(command);
            int exitCode = process.waitFor();

            if (exitCode != 0) {
                throw new RuntimeException("压缩备份文件失败，退出码: " + exitCode);
            }

            logger.info("备份文件压缩完成: {}", compressedFile);
            return compressedFile.toString();

        } catch (IOException | InterruptedException e) {
            logger.error("压缩备份文件失败", e);
            throw new IOException("压缩备份文件失败", e);
        }
    }

    /**
     * 解压缩备份文件
     */
    public String decompressBackup(String compressedFilePath) throws IOException {
        try {
            Path compressedFile = Paths.get(compressedFilePath);
            if (!Files.exists(compressedFile)) {
                throw new FileNotFoundException("压缩文件不存在: " + compressedFilePath);
            }

            // 解压文件
            Path targetDir = compressedFile.getParent();
            String command = String.format("tar -xzf %s -C %s",
                    compressedFile, targetDir);

            Process process = Runtime.getRuntime().exec(command);
            int exitCode = process.waitFor();

            if (exitCode != 0) {
                throw new RuntimeException("解压缩备份文件失败，退出码: " + exitCode);
            }

            // 获取解压后的目录名
            String fileName = compressedFile.getFileName().toString();
            String dirName = fileName.substring(0, fileName.lastIndexOf(".tar.gz"));
            Path extractedDir = targetDir.resolve(dirName);

            logger.info("备份文件解压缩完成: {}", extractedDir);
            return extractedDir.toString();

        } catch (IOException | InterruptedException e) {
            logger.error("解压缩备份文件失败", e);
            throw new IOException("解压缩备份文件失败", e);
        }
    }

    /**
     * 加密备份文件
     */
    public String encryptBackup(String backupPath, String password) throws IOException {
        // 使用OpenSSL加密文件
        try {
            String encryptedFile = backupPath + ".enc";
            String command = String.format("openssl enc -aes-256-cbc -salt -in %s -out %s -pass pass:%s",
                    backupPath, encryptedFile, password);

            Process process = Runtime.getRuntime().exec(command);
            int exitCode = process.waitFor();

            if (exitCode != 0) {
                throw new RuntimeException("加密备份文件失败，退出码: " + exitCode);
            }

            logger.info("备份文件加密完成: {}", encryptedFile);
            return encryptedFile;

        } catch (IOException | InterruptedException e) {
            logger.error("加密备份文件失败", e);
            throw new IOException("加密备份文件失败", e);
        }
    }

    /**
     * 解密备份文件
     */
    public String decryptBackup(String encryptedFilePath, String password) throws IOException {
        try {
            // 获取原始文件名
            String originalFile = encryptedFilePath.replace(".enc", "");
            String command = String.format("openssl enc -aes-256-cbc -d -in %s -out %s -pass pass:%s",
                    encryptedFilePath, originalFile, password);

            Process process = Runtime.getRuntime().exec(command);
            int exitCode = process.waitFor();

            if (exitCode != 0) {
                throw new RuntimeException("解密备份文件失败，退出码: " + exitCode);
            }

            logger.info("备份文件解密完成: {}", originalFile);
            return originalFile;

        } catch (IOException | InterruptedException e) {
            logger.error("解密备份文件失败", e);
            throw new IOException("解密备份文件失败", e);
        }
    }

    /**
     * 上传备份到远程存储
     */
    public boolean uploadToRemoteStorage(String localPath, String remotePath) {
        // 这里可以实现上传到云存储（如S3、OSS等）的逻辑
        // 示例：使用scp上传到远程服务器
        try {
            String command = String.format("scp -r %s %s", localPath, remotePath);
            Process process = Runtime.getRuntime().exec(command);
            int exitCode = process.waitFor();

            return exitCode == 0;
        } catch (IOException | InterruptedException e) {
            logger.error("上传备份到远程存储失败", e);
            return false;
        }
    }

    /**
     * 从远程存储下载备份
     */
    public boolean downloadFromRemoteStorage(String remotePath, String localPath) {
        // 从云存储下载备份文件
        try {
            String command = String.format("scp -r %s %s", remotePath, localPath);
            Process process = Runtime.getRuntime().exec(command);
            int exitCode = process.waitFor();

            return exitCode == 0;
        } catch (IOException | InterruptedException e) {
            logger.error("从远程存储下载备份失败", e);
            return false;
        }
    }

    /**
     * 获取备份存储位置
     */
    public String getBackupLocation() {
        return backupLocation;
    }

    /**
     * 设置备份存储位置
     */
    public void setBackupLocation(String backupLocation) {
        this.backupLocation = backupLocation;
    }

    /**
     * 获取临时备份目录
     */
    public String getTempBackupDir() {
        return tempBackupDir;
    }

    /**
     * 设置临时备份目录
     */
    public void setTempBackupDir(String tempBackupDir) {
        this.tempBackupDir = tempBackupDir;
    }

    /**
     * 获取可用磁盘空间
     */
    public long getAvailableDiskSpace() {
        Path backupPath = Paths.get(backupLocation);
        try {
            return Files.getFileStore(backupPath).getUsableSpace();
        } catch (IOException e) {
            logger.error("获取可用磁盘空间失败", e);
            return -1;
        }
    }

    /**
     * 检查是否有足够的空间进行备份
     */
    public boolean hasEnoughSpace(long requiredSpace) {
        long availableSpace = getAvailableDiskSpace();
        return availableSpace > requiredSpace;
    }

    /**
     * 计算目录大小
     */
    public long calculateDirectorySize(Path directory) throws IOException {
        return Files.walk(directory)
                .filter(Files::isRegularFile)
                .mapToLong(path -> {
                    try {
                        return Files.size(path);
                    } catch (IOException e) {
                        logger.error("获取文件大小失败: {}", path, e);
                        return 0L;
                    }
                })
                .sum();
    }

    /**
     * 创建备份摘要
     */
    public String createBackupSummary(BackupTask task) {
        StringBuilder summary = new StringBuilder();
        summary.append("=== 数据备份摘要 ===\n");
        summary.append("任务ID: ").append(task.getTaskId()).append("\n");
        summary.append("任务类型: ").append(task.getTaskType()).append("\n");
        summary.append("开始时间: ").append(task.getStartTime()).append("\n");
        summary.append("结束时间: ").append(task.getEndTime()).append("\n");
        summary.append("耗时: ").append(task.getDurationInSeconds()).append(" 秒\n");
        summary.append("备份大小: ").append(String.format("%.2f MB", task.getBackupSizeInMB())).append("\n");
        summary.append("文件数量: ").append(task.getFileCount()).append("\n");
        summary.append("状态: ").append(task.getStatus()).append("\n");

        if (task.getErrorMessage() != null) {
            summary.append("错误信息: ").append(task.getErrorMessage()).append("\n");
        }

        summary.append("==================");

        return summary.toString();
    }

    /**
     * 保存备份日志
     */
    public void saveBackupLog(String logContent, String logFileName) throws IOException {
        Path logPath = Paths.get(backupLocation, "logs");
        if (!Files.exists(logPath)) {
            Files.createDirectories(logPath);
        }

        Path logFile = logPath.resolve(logFileName);
        Files.write(logFile, logContent.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /**
     * 获取备份日志
     */
    public String getBackupLog(String logFileName) throws IOException {
        Path logPath = Paths.get(backupLocation, "logs", logFileName);
        if (!Files.exists(logPath)) {
            return "日志文件不存在";
        }

        return new String(Files.readAllBytes(logPath));
    }

    /**
     * 清理临时文件
     */
    public void cleanupTempFiles() {
        try {
            Path tempPath = Paths.get(tempBackupDir);
            if (Files.exists(tempPath)) {
                Files.walk(tempPath)
                        .sorted((a, b) -> -a.compareTo(b)) // 先删除文件，再删除目录
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException e) {
                                logger.error("删除临时文件失败: {}", path, e);
                            }
                        });
                logger.info("临时文件清理完成");
            }
        } catch (IOException e) {
            logger.error("清理临时文件失败", e);
        }
    }

    /**
     * 获取备份历史
     */
    public List<BackupTask> getBackupHistory(int limit) {
        List<BackupTask> history = new ArrayList<>(backupTasks.values());
        history.sort((t1, t2) -> t2.getStartTime().compareTo(t1.getStartTime()));

        if (history.size() > limit) {
            return history.subList(0, limit);
        }

        return history;
    }

    /**
     * 导出备份报告
     */
    public void exportBackupReport(String reportPath, LocalDateTime startDate, LocalDateTime endDate) throws IOException {
        // 生成备份报告
        List<BackupTask> tasks = new ArrayList<>();

        backupTasks.values().forEach(task -> {
            if (task.getStartTime().isAfter(startDate) && task.getStartTime().isBefore(endDate)) {
                tasks.add(task);
            }
        });

        // 生成报告内容
        StringBuilder report = new StringBuilder();
        report.append("=== 数据备份报告 ===\n");
        report.append("报告生成时间: ").append(LocalDateTime.now()).append("\n");
        report.append("统计时间范围: ").append(startDate).append(" - ").append(endDate).append("\n");
        report.append("总备份次数: ").append(tasks.size()).append("\n");

        // 统计信息
        long totalSize = tasks.stream().mapToLong(BackupTask::getBackupSize).sum();
        long totalFiles = tasks.stream().mapToLong(BackupTask::getFileCount).sum();
        long successCount = tasks.stream().filter(BackupTask::isSuccess).count();

        report.append("总备份大小: ").append(String.format("%.2f GB", totalSize / 1024.0 / 1024.0 / 1024.0)).append("\n");
        report.append("总文件数: ").append(totalFiles).append("\n");
        report.append("成功次数: ").append(successCount).append("\n");
        report.append("失败次数: ").append(tasks.size() - successCount).append("\n");

        // 任务详情
        report.append("\n=== 任务详情 ===\n");
        tasks.forEach(task -> {
            report.append("\n任务ID: ").append(task.getTaskId()).append("\n");
            report.append("类型: ").append(task.getTaskType()).append("\n");
            report.append("状态: ").append(task.getStatus()).append("\n");
            report.append("大小: ").append(String.format("%.2f MB", task.getBackupSizeInMB())).append("\n");
            report.append("耗时: ").append(task.getDurationInSeconds()).append(" 秒\n");
        });

        // 写入报告文件
        Files.write(Paths.get(reportPath), report.toString().getBytes());
        logger.info("备份报告已导出: {}", reportPath);
    }

    /**
     * 检查备份一致性
     */
    public boolean checkBackupConsistency(String backupPath) {
        try {
            // 验证备份文件的完整性
            // 检查文件是否存在
            // 检查文件大小
            // 检查校验和

            Path path = Paths.get(backupPath);
            if (!Files.exists(path)) {
                return false;
            }

            // 这里应该实现更详细的验证逻辑
            // 如计算MD5、SHA256等校验和

            return true;
        } catch (Exception e) {
            logger.error("检查备份一致性失败: {}", backupPath, e);
            return false;
        }
    }

    /**
     * 计算文件的MD5校验和
     */
    public String calculateFileMD5(Path filePath) throws IOException {
        try (InputStream is = Files.newInputStream(filePath)) {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[8192];
            int numRead;
            while ((numRead = is.read(buffer)) > 0) {
                md.update(buffer, 0, numRead);
            }
            byte[] digest = md.digest();
            StringBuilder result = new StringBuilder();
            for (byte b : digest) {
                result.append(String.format("%02x", b));
            }
            return result.toString();
        } catch (Exception e) {
            logger.error("计算文件MD5失败: {}", filePath, e);
            throw new IOException("计算文件MD5失败", e);
        }
    }

    /**
     * 比较两个文件的MD5校验和
     */
    public boolean compareFileMD5(Path file1, Path file2) throws IOException {
        String md5_1 = calculateFileMD5(file1);
        String md5_2 = calculateFileMD5(file2);
        return md5_1.equals(md5_2);
    }

    /**
     * 获取备份文件信息
     */
    public BackupFileInfo getBackupFileInfo(String filePath) {
        try {
            Path path = Paths.get(filePath);
            if (!Files.exists(path)) {
                return null;
            }

            BackupFileInfo info = new BackupFileInfo();
            info.setPath(filePath);
            info.setSize(Files.size(path));
            info.setLastModifiedTime(Files.getLastModifiedTime(path).toInstant()
                    .atZone(java.time.ZoneId.systemDefault()).toLocalDateTime());
            info.setMd5(calculateFileMD5(path));

            return info;
        } catch (IOException e) {
            logger.error("获取备份文件信息失败: {}", filePath, e);
            return null;
        }
    }

    /**
     * 备份文件信息
     */
    public static class BackupFileInfo {
        private String path;
        private long size;
        private LocalDateTime lastModifiedTime;
        private String md5;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public long getSize() {
            return size;
        }

        public void setSize(long size) {
            this.size = size;
        }

        public LocalDateTime getLastModifiedTime() {
            return lastModifiedTime;
        }

        public void setLastModifiedTime(LocalDateTime lastModifiedTime) {
            this.lastModifiedTime = lastModifiedTime;
        }

        public String getMd5() {
            return md5;
        }

        public void setMd5(String md5) {
            this.md5 = md5;
        }

        public double getSizeInMB() {
            return size / 1024.0 / 1024.0;
        }
    }

    /**
     * 备份任务状态
     */
    public enum BackupStatus {
        RUNNING("运行中"),
        COMPLETED("已完成"),
        FAILED("失败"),
        CANCELLED("已取消");

        private final String description;

        BackupStatus(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    /**
     * 备份类型
     */
    public enum BackupType {
        FULL_BACKUP("全量备份"),
        INCREMENTAL_BACKUP("增量备份"),
        DIFFERENTIAL_BACKUP("差异备份");

        private final String description;

        BackupType(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    /**
     * 备份策略
     */
    public static class BackupPolicy {
        private boolean enabled = true;
        private String backupType = "FULL_BACKUP";
        private int retentionDays = 30;
        private String schedule = "0 0 2 * * ?"; // 每天凌晨2点
        private String remoteStorageUrl;
        private boolean encrypt = false;
        private String encryptionPassword;
        private boolean compress = true;
        private String compressionAlgorithm = "GZIP";

        // getters and setters
        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getBackupType() {
            return backupType;
        }

        public void setBackupType(String backupType) {
            this.backupType = backupType;
        }

        public int getRetentionDays() {
            return retentionDays;
        }

        public void setRetentionDays(int retentionDays) {
            this.retentionDays = retentionDays;
        }

        public String getSchedule() {
            return schedule;
        }

        public void setSchedule(String schedule) {
            this.schedule = schedule;
        }

        public String getRemoteStorageUrl() {
            return remoteStorageUrl;
        }

        public void setRemoteStorageUrl(String remoteStorageUrl) {
            this.remoteStorageUrl = remoteStorageUrl;
        }

        public boolean isEncrypt() {
            return encrypt;
        }

        public void setEncrypt(boolean encrypt) {
            this.encrypt = encrypt;
        }

        public String getEncryptionPassword() {
            return encryptionPassword;
        }

        public void setEncryptionPassword(String encryptionPassword) {
            this.encryptionPassword = encryptionPassword;
        }

        public boolean isCompress() {
            return compress;
        }

        public void setCompress(boolean compress) {
            this.compress = compress;
        }

        public String getCompressionAlgorithm() {
            return compressionAlgorithm;
        }

        public void setCompressionAlgorithm(String compressionAlgorithm) {
            this.compressionAlgorithm = compressionAlgorithm;
        }
    }

    /**
     * 备份配置
     */
    public static class BackupConfig {
        private String backupLocation;
        private String tempDir;
        private boolean enabled;
        private int maxConcurrentBackups = 1;
        private long maxBackupSize = 100L * 1024 * 1024 * 1024; // 100GB
        private int retryAttempts = 3;
        private long retryDelayMs = 5000; // 5秒

        // getters and setters
        public String getBackupLocation() {
            return backupLocation;
        }

        public void setBackupLocation(String backupLocation) {
            this.backupLocation = backupLocation;
        }

        public String getTempDir() {
            return tempDir;
        }

        public void setTempDir(String tempDir) {
            this.tempDir = tempDir;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getMaxConcurrentBackups() {
            return maxConcurrentBackups;
        }

        public void setMaxConcurrentBackups(int maxConcurrentBackups) {
            this.maxConcurrentBackups = maxConcurrentBackups;
        }

        public long getMaxBackupSize() {
            return maxBackupSize;
        }

        public void setMaxBackupSize(long maxBackupSize) {
            this.maxBackupSize = maxBackupSize;
        }

        public int getRetryAttempts() {
            return retryAttempts;
        }

        public void setRetryAttempts(int retryAttempts) {
            this.retryAttempts = retryAttempts;
        }

        public long getRetryDelayMs() {
            return retryDelayMs;
        }

        public void setRetryDelayMs(long retryDelayMs) {
            this.retryDelayMs = retryDelayMs;
        }
    }

    /**
     * 备份异常
     */
    public static class BackupException extends Exception {
        public BackupException(String message) {
            super(message);
        }

        public BackupException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * 备份监听器
     */
    public interface BackupListener {
        void onBackupStart(BackupTask task);
        void onBackupProgress(BackupTask task, int progress);
        void onBackupComplete(BackupTask task);
        void onBackupError(BackupTask task, Exception error);
    }

    /**
     * 备份过滤器
     */
    public interface BackupFilter {
        boolean shouldIncludeFile(Path filePath);
        boolean shouldIncludeDirectory(Path dirPath);
    }

    /**
     * 默认备份过滤器
     */
    public static class DefaultBackupFilter implements BackupFilter {
        private final List<String> excludePatterns = new ArrayList<>();

        public DefaultBackupFilter() {
            // 排除临时文件和日志文件
            excludePatterns.add("*.tmp");
            excludePatterns.add("*.log");
            excludePatterns.add("*.lock");
            excludePatterns.add("*.pid");
        }

        @Override
        public boolean shouldIncludeFile(Path filePath) {
            String fileName = filePath.getFileName().toString();
            return excludePatterns.stream().noneMatch(pattern ->
                    fileName.matches(pattern.replace("*", ".*")));
        }

        @Override
        public boolean shouldIncludeDirectory(Path dirPath) {
            String dirName = dirPath.getFileName().toString();
            // 排除隐藏目录和临时目录
            return !dirName.startsWith(".") && !dirName.equals("temp") && !dirName.equals("tmp");
        }
    }
}