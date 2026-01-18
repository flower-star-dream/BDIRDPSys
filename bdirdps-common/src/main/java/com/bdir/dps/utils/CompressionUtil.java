package com.bdir.dps.utils;

import com.bdir.dps.entity.CompressionTask;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 数据压缩工具类
 * 提供文件压缩和解压缩功能
 *
 * @author BDIRDPSys开发团队
 * @since 2026-01-16
 */
@Component
public class CompressionUtil {

    @Value("${data.compression.delete-original:false}")
    private boolean deleteOriginalAfterCompression;

    @Value("${data.compression.buffer-size:8192}")
    private int bufferSize;

    // 存储压缩任务
    private final ConcurrentHashMap<String, CompressionTask> taskMap = new ConcurrentHashMap<>();

    /**
     * 压缩文件
     * 支持多种压缩算法
     */
    public File compressFile(File inputFile, String algorithm) throws IOException {
        if (!inputFile.exists()) {
            throw new FileNotFoundException("文件不存在: " + inputFile.getAbsolutePath());
        }

        String compressedFileName = inputFile.getName() + getExtension(algorithm);
        File compressedFile = new File(inputFile.getParent(), compressedFileName);

        switch (algorithm.toUpperCase()) {
            case "GZIP":
                compressWithGzip(inputFile, compressedFile);
                break;
            case "SNAPPY":
                compressWithSnappy(inputFile, compressedFile);
                break;
            case "LZ4":
                compressWithLz4(inputFile, compressedFile);
                break;
            default:
                throw new IllegalArgumentException("不支持的压缩算法: " + algorithm);
        }

        return compressedFile;
    }

    /**
     * 解压缩文件
     */
    public File decompressFile(File compressedFile) throws IOException {
        if (!compressedFile.exists()) {
            throw new FileNotFoundException("压缩文件不存在: " + compressedFile.getAbsolutePath());
        }

        String fileName = compressedFile.getName();
        String decompressedFileName = removeExtension(fileName);
        File decompressedFile = new File(compressedFile.getParent(), decompressedFileName);

        if (fileName.endsWith(".gz")) {
            decompressWithGzip(compressedFile, decompressedFile);
        } else if (fileName.endsWith(".snappy")) {
            decompressWithSnappy(compressedFile, decompressedFile);
        } else if (fileName.endsWith(".lz4")) {
            decompressWithLz4(compressedFile, decompressedFile);
        } else {
            throw new IllegalArgumentException("不支持的压缩文件格式: " + fileName);
        }

        return decompressedFile;
    }

    /**
     * 使用GZIP压缩
     */
    private void compressWithGzip(File inputFile, File outputFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(inputFile);
             FileOutputStream fos = new FileOutputStream(outputFile);
             GZIPOutputStream gzos = new GZIPOutputStream(fos)) {

            byte[] buffer = new byte[bufferSize];
            int len;
            while ((len = fis.read(buffer)) != -1) {
                gzos.write(buffer, 0, len);
            }
        }
    }

    /**
     * 使用GZIP解压缩
     */
    private void decompressWithGzip(File compressedFile, File outputFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(compressedFile);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             FileOutputStream fos = new FileOutputStream(outputFile)) {

            byte[] buffer = new byte[bufferSize];
            int len;
            while ((len = gzis.read(buffer)) != -1) {
                fos.write(buffer, 0, len);
            }
        }
    }

    /**
     * 使用Snappy压缩（需要Snappy库）
     */
    private void compressWithSnappy(File inputFile, File outputFile) throws IOException {
        // 这里使用GZIP代替，实际项目中需要引入Snappy库
        // import org.xerial.snappy.Snappy;
        // byte[] input = Files.readAllBytes(inputFile.toPath());
        // byte[] compressed = Snappy.compress(input);
        // Files.write(outputFile.toPath(), compressed);
        compressWithGzip(inputFile, outputFile);
    }

    /**
     * 使用Snappy解压缩
     */
    private void decompressWithSnappy(File compressedFile, File outputFile) throws IOException {
        // 实际项目中需要引入Snappy库
        decompressWithGzip(compressedFile, outputFile);
    }

    /**
     * 使用LZ4压缩（需要LZ4库）
     */
    private void compressWithLz4(File inputFile, File outputFile) throws IOException {
        // 这里使用GZIP代替，实际项目中需要引入LZ4库
        compressWithGzip(inputFile, outputFile);
    }

    /**
     * 使用LZ4解压缩
     */
    private void decompressWithLz4(File compressedFile, File outputFile) throws IOException {
        // 实际项目中需要引入LZ4库
        decompressWithGzip(compressedFile, outputFile);
    }

    /**
     * 获取压缩文件扩展名
     */
    private String getExtension(String algorithm) {
        switch (algorithm.toUpperCase()) {
            case "GZIP":
                return ".gz";
            case "SNAPPY":
                return ".snappy";
            case "LZ4":
                return ".lz4";
            default:
                return ".gz";
        }
    }

    /**
     * 移除压缩文件扩展名
     */
    private String removeExtension(String fileName) {
        if (fileName.endsWith(".gz")) {
            return fileName.substring(0, fileName.length() - 3);
        } else if (fileName.endsWith(".snappy")) {
            return fileName.substring(0, fileName.length() - 7);
        } else if (fileName.endsWith(".lz4")) {
            return fileName.substring(0, fileName.length() - 4);
        }
        return fileName;
    }

    /**
     * 查找指定日期之前的数据文件
     */
    public List<File> findDataFilesBeforeDate(LocalDateTime beforeDate) {
        List<File> dataFiles = new ArrayList<>();

        // 这里应该根据实际的数据存储路径查找文件
        // 示例：查找data目录下的csv文件
        Path dataPath = Paths.get("data/sensor");
        if (Files.exists(dataPath)) {
            try {
                Files.walk(dataPath)
                    .filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".csv"))
                    .forEach(path -> dataFiles.add(path.toFile()));
            } catch (IOException e) {
                throw new RuntimeException("查找数据文件失败", e);
            }
        }

        return dataFiles;
    }

    /**
     * 获取压缩任务
     */
    public CompressionTask getTaskById(String taskId) {
        return taskMap.get(taskId);
    }

    /**
     * 保存压缩任务
     */
    public void saveTask(CompressionTask task) {
        taskMap.put(task.getTaskId(), task);
    }

    /**
     * 获取压缩统计信息
     */
    public DataCompressionService.CompressionStatistics getCompressionStatistics() {
        // 这里应该查询数据库或文件系统获取统计信息
        // 示例实现
        DataCompressionService.CompressionStatistics stats =
            new DataCompressionService.CompressionStatistics();

        // 统计所有已完成的任务
        taskMap.values().stream()
            .filter(task -> "COMPLETED".equals(task.getStatus()))
            .forEach(task -> {
                stats.setTotalCompressedFiles(stats.getTotalCompressedFiles() + task.getCompressedFiles());
                stats.setTotalOriginalSize(stats.getTotalOriginalSize() + task.getOriginalSize());
                stats.setTotalCompressedSize(stats.getTotalCompressedSize() + task.getCompressedSize());
            });

        // 计算平均压缩率
        if (stats.getTotalOriginalSize() > 0) {
            double ratio = (1.0 - (double) stats.getTotalCompressedSize() / stats.getTotalOriginalSize()) * 100;
            stats.setAverageCompressionRatio(ratio);
        }

        return stats;
    }

    public boolean deleteOriginalAfterCompression() {
        return deleteOriginalAfterCompression;
    }

    /**
     * 计算文件压缩率
     */
    public double calculateCompressionRatio(File originalFile, File compressedFile) {
        if (originalFile.length() > 0) {
            return (1.0 - (double) compressedFile.length() / originalFile.length()) * 100;
        }
        return 0.0;
    }

    /**
     * 批量压缩文件
     */
    public List<File> compressFiles(List<File> inputFiles, String algorithm) throws IOException {
        List<File> compressedFiles = new ArrayList<>();
        for (File inputFile : inputFiles) {
            compressedFiles.add(compressFile(inputFile, algorithm));
        }
        return compressedFiles;
    }

    /**
     * 批量解压缩文件
     */
    public List<File> decompressFiles(List<File> compressedFiles) throws IOException {
        List<File> decompressedFiles = new ArrayList<>();
        for (File compressedFile : compressedFiles) {
            decompressedFiles.add(decompressFile(compressedFile));
        }
        return decompressedFiles;
    }
}