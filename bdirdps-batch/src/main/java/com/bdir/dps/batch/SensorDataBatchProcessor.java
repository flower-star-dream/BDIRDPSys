package com.bdir.dps.batch;

import com.bdir.dps.mapper.HiveQueryRouterMapper;
import com.bdir.dps.mapper.MySQLMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 传感器数据批处理任务
 * 负责定期处理历史数据、生成报表、数据归档等批处理操作
 */
@Slf4j
@Component
@EnableBatchProcessing
public class SensorDataBatchProcessor {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private MySQLMapper mysqlMapper;

    @Autowired
    private HiveQueryRouterMapper hiveQueryRouterMapper;

    @Value("${batch.processing.chunk-size:1000}")
    private int chunkSize;

    @Value("${batch.processing.retention-days:90}")
    private int retentionDays;

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * 每日数据汇总任务
     * 每天凌晨2点执行
     */
    @Scheduled(cron = "0 0 2 * * ?")
    public void runDailyAggregation() {
        log.info("开始执行每日数据汇总任务");

        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("jobDate", LocalDateTime.now().format(DATE_FORMATTER))
                    .addLong("timestamp", System.currentTimeMillis())
                    .toJobParameters();

            JobExecution execution = jobLauncher.run(dailyAggregationJob(), jobParameters);
            log.info("每日数据汇总任务执行完成，状态: {}", execution.getStatus());
        } catch (Exception e) {
            log.error("每日数据汇总任务执行失败", e);
        }
    }

    /**
     * 每周数据报表任务
     * 每周一凌晨3点执行
     */
    @Scheduled(cron = "0 0 3 * * MON")
    public void runWeeklyReport() {
        log.info("开始执行每周数据报表任务");

        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("jobWeek", String.valueOf(LocalDateTime.now().getDayOfYear() / 7 + 1))
                    .addLong("timestamp", System.currentTimeMillis())
                    .toJobParameters();

            JobExecution execution = jobLauncher.run(weeklyReportJob(), jobParameters);
            log.info("每周数据报表任务执行完成，状态: {}", execution.getStatus());
        } catch (Exception e) {
            log.error("每周数据报表任务执行失败", e);
        }
    }

    /**
     * 数据清理任务
     * 每月1号凌晨4点执行
     */
    @Scheduled(cron = "0 0 4 1 * ?")
    public void runDataCleanup() {
        log.info("开始执行数据清理任务");

        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("cleanupDate", LocalDateTime.now().format(DATE_FORMATTER))
                    .addLong("timestamp", System.currentTimeMillis())
                    .toJobParameters();

            JobExecution execution = jobLauncher.run(dataCleanupJob(), jobParameters);
            log.info("数据清理任务执行完成，状态: {}", execution.getStatus());
        } catch (Exception e) {
            log.error("数据清理任务执行失败", e);
        }
    }

    /**
     * 定义每日数据汇总Job
     */
    @Bean
    public Job dailyAggregationJob() {
        return jobBuilderFactory.get("dailyAggregationJob")
                .incrementer(new RunIdIncrementer())
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {
                        log.info("每日数据汇总Job开始执行");
                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        log.info("每日数据汇总Job执行完成，状态: {}", jobExecution.getStatus());
                    }
                })
                .start(dailyAggregationStep())
                .build();
    }

    /**
     * 定义每日数据汇总Step
     */
    @Bean
    public Step dailyAggregationStep() {
        return stepBuilderFactory.get("dailyAggregationStep")
                .<Map<String, Object>, Map<String, Object>>chunk(chunkSize)
                .reader(dailyDataReader())
                .processor(dailyDataProcessor())
                .writer(dailyDataWriter())
                .faultTolerant()
                .retryLimit(3)
                .retry(Exception.class)
                .skipLimit(100)
                .skip(Exception.class)
                .listener(new ChunkListener() {
                    @Override
                    public void beforeChunk(ChunkContext context) {
                        log.debug("开始处理数据块");
                    }

                    @Override
                    public void afterChunk(ChunkContext context) {
                        log.debug("完成处理数据块，共处理 {} 条记录",
                                context.getStepContext().getStepExecution().getReadCount());
                    }

                    @Override
                    public void afterChunkError(ChunkContext context) {
                        log.error("处理数据块发生错误");
                    }
                })
                .build();
    }

    /**
     * 每日数据读取器
     */
    @Bean
    public ItemReader<Map<String, Object>> dailyDataReader() {
        return new ItemReader<Map<String, Object>>() {
            private LocalDateTime currentDate = LocalDateTime.now().minusDays(1);
            private boolean processed = false;

            @Override
            public Map<String, Object> read() throws Exception {
                if (processed) {
                    return null; // 结束读取
                }

                // 查询昨天的数据
                String startTime = currentDate.withHour(0).withMinute(0).withSecond(0).toString();
                String endTime = currentDate.withHour(23).withMinute(59).withSecond(59).toString();

                List<Map<String, Object>> dailyStats = hiveQueryRouterMapper.routeQuery(
                    startTime, endTime, null, null, Arrays.asList("temperature", "humidity", "pressure")
                );

                processed = true;

                Map<String, Object> result = new HashMap<>();
                result.put("date", currentDate.format(DATE_FORMATTER));
                result.put("stats", dailyStats);
                result.put("totalRecords", dailyStats.size());

                return result;
            }
        };
    }

    /**
     * 每日数据处理器
     */
    @Bean
    public ItemProcessor<Map<String, Object>, Map<String, Object>> dailyDataProcessor() {
        return new ItemProcessor<Map<String, Object>, Map<String, Object>>() {
            @Override
            public Map<String, Object> process(Map<String, Object> item) throws Exception {
                log.info("处理 {} 的日汇总数据，共 {} 条统计记录",
                        item.get("date"), item.get("totalRecords"));

                // 可以在这里添加更复杂的数据处理逻辑
                // 例如：计算趋势、异常检测、生成指标等

                return item;
            }
        };
    }

    /**
     * 每日数据写入器
     */
    @Bean
    public ItemWriter<Map<String, Object>> dailyDataWriter() {
        return new ItemWriter<Map<String, Object>>() {
            @Override
            public void write(List<? extends Map<String, Object>> items) throws Exception {
                for (Map<String, Object> item : items) {
                    String date = (String) item.get("date");
                    List<Map<String, Object>> stats = (List<Map<String, Object>>) item.get("stats");

                    log.info("写入 {} 的日汇总数据到Hive，共 {} 条记录", date, stats.size());

                    // 将汇总数据写入Hive的日汇总表
                    writeDailySummaryToHive(date, stats);
                }
            }

            private void writeDailySummaryToHive(String date, List<Map<String, Object>> stats) {
                // 这里实现写入Hive的逻辑
                // 可以调用HiveQueryRouterMapper的相关方法
                log.info("已将 {} 的日汇总数据写入Hive", date);
            }
        };
    }

    /**
     * 定义每周数据报表Job
     */
    @Bean
    public Job weeklyReportJob() {
        return jobBuilderFactory.get("weeklyReportJob")
                .incrementer(new RunIdIncrementer())
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {
                        log.info("每周数据报表Job开始执行");
                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        log.info("每周数据报表Job执行完成，状态: {}", jobExecution.getStatus());
                    }
                })
                .start(weeklyReportStep())
                .build();
    }

    /**
     * 定义每周数据报表Step
     */
    @Bean
    public Step weeklyReportStep() {
        return stepBuilderFactory.get("weeklyReportStep")
                .<Map<String, Object>, String>chunk(chunkSize)
                .reader(weeklyDataReader())
                .processor(weeklyDataProcessor())
                .writer(weeklyReportWriter())
                .build();
    }

    /**
     * 每周数据读取器
     */
    @Bean
    public ItemReader<Map<String, Object>> weeklyDataReader() {
        return new ItemReader<Map<String, Object>>() {
            private int weekNumber = LocalDateTime.now().getDayOfYear() / 7;
            private boolean processed = false;

            @Override
            public Map<String, Object> read() throws Exception {
                if (processed) {
                    return null;
                }

                // 查询本周的数据
                LocalDateTime startOfWeek = LocalDateTime.now().minusDays(7);
                LocalDateTime endOfWeek = LocalDateTime.now();

                List<Map<String, Object>> weeklyStats = hiveQueryRouterMapper.routeQuery(
                    startOfWeek.toString(), endOfWeek.toString(),
                    null, null, Arrays.asList("temperature", "humidity", "pressure")
                );

                processed = true;

                Map<String, Object> result = new HashMap<>();
                result.put("weekNumber", weekNumber);
                result.put("stats", weeklyStats);
                result.put("totalRecords", weeklyStats.size());
                result.put("startDate", startOfWeek.format(DATE_FORMATTER));
                result.put("endDate", endOfWeek.format(DATE_FORMATTER));

                return result;
            }
        };
    }

    /**
     * 每周数据处理器
     */
    @Bean
    public ItemProcessor<Map<String, Object>, String> weeklyDataProcessor() {
        return new ItemProcessor<Map<String, Object>, String>() {
            @Override
            public String process(Map<String, Object> item) throws Exception {
                String weekNumber = item.get("weekNumber").toString();
                String startDate = (String) item.get("startDate");
                String endDate = (String) item.get("endDate");
                int totalRecords = (Integer) item.get("totalRecords");

                log.info("生成第 {} 周的数据报表 ({} - {})，共 {} 条记录",
                        weekNumber, startDate, endDate, totalRecords);

                // 生成周报内容
                String reportContent = generateWeeklyReport(item);

                return reportContent;
            }

            private String generateWeeklyReport(Map<String, Object> data) {
                // 这里实现报表生成逻辑
                StringBuilder report = new StringBuilder();
                report.append("=== 传感器数据周报 ===\n");
                report.append("统计周期: ").append(data.get("startDate")).append(" 至 ").append(data.get("endDate")).append("\n");
                report.append("数据记录数: ").append(data.get("totalRecords")).append("\n");
                report.append("生成时间: ").append(LocalDateTime.now()).append("\n");
                report.append("========================\n");

                // 添加更多统计信息...

                return report.toString();
            }
        };
    }

    /**
     * 每周报表写入器
     */
    @Bean
    public ItemWriter<String> weeklyReportWriter() {
        return new ItemWriter<String>() {
            @Override
            public void write(List<? extends String> items) throws Exception {
                for (String report : items) {
                    // 将报表保存到文件或发送到指定邮箱
                    saveWeeklyReport(report);
                }
            }

            private void saveWeeklyReport(String report) {
                // 实现报表保存逻辑
                log.info("保存周报内容:\n{}", report);
            }
        };
    }

    /**
     * 定义数据清理Job
     */
    @Bean
    public Job dataCleanupJob() {
        return jobBuilderFactory.get("dataCleanupJob")
                .incrementer(new RunIdIncrementer())
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {
                        log.info("数据清理Job开始执行");
                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        log.info("数据清理Job执行完成，状态: {}", jobExecution.getStatus());
                    }
                })
                .start(dataCleanupStep())
                .build();
    }

    /**
     * 定义数据清理Step
     */
    @Bean
    public Step dataCleanupStep() {
        return stepBuilderFactory.get("dataCleanupStep")
                .tasklet((contribution, chunkContext) -> {
                    log.info("开始清理过期数据");

                    // 计算过期时间（90天前）
                    LocalDateTime expireTime = LocalDateTime.now().minusDays(retentionDays);

                    // 删除MySQL中的过期数据
                    int deletedCount = mysqlMapper.deleteSensorData(expireTime);
                    log.info("从MySQL删除 {} 条过期数据", deletedCount);

                    // 删除Hive中的过期数据分区
                    deleteExpiredHivePartitions(expireTime);

                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    /**
     * 删除Hive中过期的分区
     */
    private void deleteExpiredHivePartitions(LocalDateTime expireTime) {
        // 实现删除Hive过期分区的逻辑
        String expireDate = expireTime.format(DATE_FORMATTER);
        log.info("删除Hive中 {} 之前的分区数据", expireDate);

        // 可以调用HiveQueryRouterMapper的方法来删除分区
    }
}

/**
 * 批处理配置类
 */
@Configuration
class BatchConfiguration {

    /**
     * 批处理监听器
     */
    @Bean
    public JobExecutionListener batchJobListener() {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                log.info("批处理任务开始: {}", jobExecution.getJobInstance().getJobName());
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                log.info("批处理任务结束: {}，状态: {}",
                        jobExecution.getJobInstance().getJobName(),
                        jobExecution.getStatus());

                // 记录任务执行时间
                long duration = jobExecution.getEndTime().getTime() -
                               jobExecution.getStartTime().getTime();
                log.info("任务执行耗时: {}ms", duration);
            }
        };
    }
}

/**
 * 批处理异常
 */
class BatchProcessingException extends RuntimeException {
    public BatchProcessingException(String message) {
        super(message);
    }

    public BatchProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * 数据读取异常
 */
class DataReadException extends BatchProcessingException {
    public DataReadException(String message) {
        super(message);
    }

    public DataReadException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * 数据写入异常
 */
class DataWriteException extends BatchProcessingException {
    public DataWriteException(String message) {
        super(message);
    }

    public DataWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * 批处理监控器
 */
@Slf4j
class BatchProcessingMonitor {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(BatchProcessingMonitor.class);

    private long totalJobs = 0;
    private long successfulJobs = 0;
    private long failedJobs = 0;
    private long totalRecords = 0;
    private long failedRecords = 0;

    public synchronized void recordJobStart() {
        totalJobs++;
    }

    public synchronized void recordJobSuccess() {
        successfulJobs++;
    }

    public synchronized void recordJobFailure() {
        failedJobs++;
    }

    public synchronized void recordRecordsProcessed(long records) {
        totalRecords += records;
    }

    public synchronized void recordRecordsFailed(long records) {
        failedRecords += records;
    }

    public synchronized Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalJobs", totalJobs);
        stats.put("successfulJobs", successfulJobs);
        stats.put("failedJobs", failedJobs);
        stats.put("totalRecords", totalRecords);
        stats.put("failedRecords", failedRecords);
        stats.put("successRate", totalJobs > 0 ? (double) successfulJobs / totalJobs * 100 : 0);
        stats.put("recordSuccessRate", totalRecords > 0 ? (double) (totalRecords - failedRecords) / totalRecords * 100 : 0);
        return stats;
    }

    public synchronized void reset() {
        totalJobs = 0;
        successfulJobs = 0;
        failedJobs = 0;
        totalRecords = 0;
        failedRecords = 0;
    }
}

/**
 * 批处理工具类
 */
class BatchUtils {
    /**
     * 生成分区值
     */
    public static String generatePartitionValue(LocalDateTime dateTime, String partitionType) {
        switch (partitionType.toLowerCase()) {
            case "daily":
                return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            case "hourly":
                return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"));
            case "monthly":
                return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM"));
            default:
                return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        }
    }

    /**
     * 计算批处理分片
     */
    public static int[] calculatePartitions(int totalRecords, int chunkSize) {
        int chunks = (int) Math.ceil((double) totalRecords / chunkSize);
        int[] partitions = new int[chunks];

        for (int i = 0; i < chunks; i++) {
            partitions[i] = Math.min(chunkSize, totalRecords - i * chunkSize);
        }

        return partitions;
    }

    /**
     * 格式化批处理进度
     */
    public static String formatProgress(long processed, long total) {
        if (total == 0) return "0%";
        double percentage = (double) processed / total * 100;
        return String.format("%.2f%% (%d/%d)", percentage, processed, total);
    }
}

/**
 * 批处理结果
 */
class BatchProcessingResult {
    private boolean success;
    private long processedRecords;
    private long failedRecords;
    private String message;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Map<String, Object> metadata;

    public BatchProcessingResult(boolean success) {
        this.success = success;
        this.startTime = LocalDateTime.now();
    }

    // Getters and setters
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }

    public long getProcessedRecords() { return processedRecords; }
    public void setProcessedRecords(long processedRecords) { this.processedRecords = processedRecords; }

    public long getFailedRecords() { return failedRecords; }
    public void setFailedRecords(long failedRecords) { this.failedRecords = failedRecords; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public LocalDateTime getStartTime() { return startTime; }
    public LocalDateTime getEndTime() { return endTime; }
    public void setEndTime(LocalDateTime endTime) { this.endTime = endTime; }

    public Map<String, Object> getMetadata() { return metadata; }
    public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }

    public long getDuration() {
        if (startTime != null && endTime != null) {
            return java.time.Duration.between(startTime, endTime).toMillis();
        }
        return 0;
    }

    @Override
    public String toString() {
        return "BatchProcessingResult{" +
                "success=" + success +
                ", processedRecords=" + processedRecords +
                ", failedRecords=" + failedRecords +
                ", message='" + message + '\'' +
                ", duration=" + getDuration() + "ms" +
                '}';
    }
}