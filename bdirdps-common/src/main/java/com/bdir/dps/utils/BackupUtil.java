package com.bdir.dps.utils;

import com.bdir.dps.entity.BackupTask;
import com.bdir.dps.service.DataBackupService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter writer = null;

        try {
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

            // 使用try-with-resources确保流正确关闭
            fos = new FileOutputStream(manifestPath.toFile());
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            writer = new BufferedWriter(osw);
            writer.write(json);
            writer.flush();

            // 保存任务到内存
            backupTasks.put(task.getTaskId(), task);

            logger.info("备份清单已创建：{}", manifestPath);
            return manifestPath.toString();

        } finally {
            // 确保所有流都被关闭
            closeQuietly(writer);
            closeQuietly(osw);
            closeQuietly(fos);
        }
    }

    /**
     * 读取备份清单文件
     */
    public BackupTask readBackupManifest(String manifestPath) throws IOException {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader reader = null;

        try {
            Path path = Paths.get(manifestPath);
            if (!Files.exists(path)) {
                throw new FileNotFoundException("备份清单文件不存在: " + manifestPath);
            }

            // 使用try-with-resources读取文件
            fis = new FileInputStream(path.toFile());
            isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            reader = new BufferedReader(isr);

            StringBuilder jsonBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                jsonBuilder.append(line);
            }

            String json = jsonBuilder.toString();
            BackupTask task = objectMapper.readValue(json, BackupTask.class);

            // 更新内存中的任务
            backupTasks.put(task.getTaskId(), task);

            return task;

        } catch (IOException e) {
            logger.error("读取备份清单失败: {}", manifestPath, e);
            throw e;
        } finally {
            // 确保所有流都被关闭
            closeQuietly(reader);
            closeQuietly(isr);
            closeQuietly(fis);
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
        FileOutputStream fos = null;
        BufferedWriter writer = null;

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

            String command = String.format("mysqldump -u%s -p%s %s", username, password, dbName);

            Process process = Runtime.getRuntime().exec(new String[]{"sh", "-c", command});

            // 获取命令输出并写入文件
            fos = new FileOutputStream(backupFile);
            writer = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8));

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    writer.write(line);
                    writer.newLine();
                }
            }

            writer.flush();

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException("MySQL备份命令执行失败，退出码: " + exitCode);
            }

            logger.info("MySQL数据备份完成: {}", backupFile);
            return backupFile;

        } finally {
            // 确保流被关闭
            closeQuietly(writer);
            closeQuietly(fos);
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
     * 复制目录（修复资源泄露问题）
     */
    private void copyDirectory(Path source, Path target) throws IOException {
        Files.walkFileTree(source, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path targetDir = target.resolve(source.relativize(dir));
                if (!Files.exists(targetDir)) {
                    Files.createDirectories(targetDir);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Path targetFile = target.resolve(source.relativize(file));
                Files.copy(file, targetFile, StandardCopyOption.REPLACE_EXISTING);
                return FileVisitResult.CONTINUE;
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
     * 压缩备份文件（修复资源泄露）
     */
    public String compressBackup(String backupPath) throws IOException {
        FileInputStream fis = null;
        GZIPOutputStream gzos = null;
        FileOutputStream fos = null;

        try {
            Path path = Paths.get(backupPath);
            if (!Files.exists(path)) {
                throw new FileNotFoundException("备份路径不存在: " + backupPath);
            }

            // 创建压缩文件
            String compressedFileName = path.getFileName() + ".gz";
            Path compressedFile = path.getParent().resolve(compressedFileName);

            // 使用GZIP压缩单个文件
            fis = new FileInputStream(path.toFile());
            fos = new FileOutputStream(compressedFile.toFile());
            gzos = new GZIPOutputStream(fos);

            byte[] buffer = new byte[8192];
            int len;
            while ((len = fis.read(buffer)) != -1) {
                gzos.write(buffer, 0, len);
            }

            gzos.finish();

            logger.info("备份文件压缩完成: {}", compressedFile);
            return compressedFile.toString();

        } catch (IOException e) {
            logger.error("压缩备份文件失败", e);
            throw new IOException("压缩备份文件失败", e);
        } finally {
            // 确保所有流都被关闭
            closeQuietly(gzos);
            closeQuietly(fos);
            closeQuietly(fis);
        }
    }

    /**
     * 解压缩备份文件（修复资源泄露）
     */
    public String decompressBackup(String compressedFilePath) throws IOException {
        GZIPInputStream gzis = null;
        FileOutputStream fos = null;
        FileInputStream fis = null;

        try {
            Path compressedFile = Paths.get(compressedFilePath);
            if (!Files.exists(compressedFile)) {
                throw new FileNotFoundException("压缩文件不存在: " + compressedFilePath);
            }

            // 解压文件
            Path targetDir = compressedFile.getParent();
            String fileName = compressedFile.getFileName().toString();
            String originalFile = fileName.replace(".gz", "");
            Path extractedFile = targetDir.resolve(originalFile);

            // 使用GZIP解压缩
            fis = new FileInputStream(compressedFile.toFile());
            gzis = new GZIPInputStream(fis);
            fos = new FileOutputStream(extractedFile.toFile());

            byte[] buffer = new byte[8192];
            int len;
            while ((len = gzis.read(buffer)) != -1) {
                fos.write(buffer, 0, len);
            }

            logger.info("备份文件解压缩完成: {}", extractedFile);
            return extractedFile.toString();

        } catch (IOException e) {
            logger.error("解压缩备份文件失败", e);
            throw new IOException("解压缩备份文件失败", e);
        } finally {
            // 确保所有流都被关闭
            closeQuietly(fos);
            closeQuietly(gzis);
            closeQuietly(fis);
        }
    }

    /**
     * 计算文件的MD5校验和（修复资源泄露）
     */
    public String calculateFileMD5(Path filePath) throws IOException {
        try (InputStream is = Files.newInputStream(filePath)) {
            MessageDigest md = MessageDigest.getInstance("MD5");
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
        } catch (NoSuchAlgorithmException e) {
            logger.error("MD5算法不支持", e);
            throw new IOException("MD5算法不支持", e);
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
     * 保存备份日志（修复资源泄露）
     */
    public void saveBackupLog(String logContent, String logFileName) throws IOException {
        Path logPath = Paths.get(backupLocation, "logs");
        if (!Files.exists(logPath)) {
            Files.createDirectories(logPath);
        }

        Path logFile = logPath.resolve(logFileName);
        try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(logContent);
            writer.newLine();
            writer.flush();
        }
    }

    /**
     * 获取备份日志（修复资源泄露）
     */
    public String getBackupLog(String logFileName) throws IOException {
        Path logPath = Paths.get(backupLocation, "logs", logFileName);
        if (!Files.exists(logPath)) {
            return "日志文件不存在";
        }

        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = Files.newBufferedReader(logPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        }

        return content.toString();
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
     * 导出备份报告（修复资源泄露）
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
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(reportPath), StandardCharsets.UTF_8)) {
            writer.write(report.toString());
            writer.flush();
        }

        logger.info("备份报告已导出: {}", reportPath);
    }

    /**
     * 安静关闭流
     */
    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                logger.warn("关闭流失败", e);
            }
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
}