package com.bdir.dps.service;

import com.bdir.dps.entity.SensorData;
import com.bdir.dps.mapper.MySQLMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * 传感器数据流处理服务
 * 负责消费Kafka中的传感器数据并进行实时处理
 */
@Slf4j
@Service
public class SensorDataStreamService {

    @Autowired
    private KafkaConsumer<String, String> kafkaConsumer;

    @Autowired
    private MySQLMapper mysqlMapper;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${spring.kafka.consumer.sensor-topics:sensor-data-temperature,sensor-data-humidity,sensor-data-pressure}")
    private String[] sensorTopics;

    @Value("${stream.processing.batch-size:100}")
    private int batchSize;

    @Value("${stream.processing.flush-interval:5000}")
    private long flushInterval;

    private final ExecutorService executorService = Executors.newFixedThreadPool(3);
    private final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);

    private volatile boolean running = false;
    private final List<SensorData> dataBuffer = new ArrayList<>();
    private final ReentrantLock bufferLock = new ReentrantLock();

    /**
     * 初始化服务
     */
    @PostConstruct
    public void init() {
        log.info("初始化传感器数据流处理服务");

        // 订阅主题
        kafkaConsumer.subscribe(Arrays.asList(sensorTopics));
        log.info("已订阅Kafka主题: {}", Arrays.toString(sensorTopics));

        // 启动消费线程
        startConsumerThread();

        // 启动定时刷新任务
        startScheduledFlush();

        running = true;
    }

    /**
     * 启动Kafka消费线程
     */
    private void startConsumerThread() {
        executorService.submit(() -> {
            log.info("Kafka消费线程已启动");

            try {
                while (running) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            // 解析消息
                            SensorData sensorData = objectMapper.readValue(record.value(), SensorData.class);

                            // 数据验证
                            if (validateSensorData(sensorData)) {
                                // 添加到缓冲区
                                bufferData(sensorData);
                            } else {
                                log.warn("无效的传感器数据: {}", record.value());
                            }
                        } catch (Exception e) {
                            log.error("处理Kafka消息失败: {}", record.value(), e);
                        }
                    }
                }
            } catch (WakeupException e) {
                // 正常关闭
                log.info("Kafka消费线程被唤醒，准备关闭");
            } catch (Exception e) {
                log.error("Kafka消费线程异常", e);
            } finally {
                kafkaConsumer.close();
                log.info("Kafka消费线程已关闭");
            }
        });
    }

    /**
     * 启动定时刷新任务
     */
    private void startScheduledFlush() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                flushBuffer();
            } catch (Exception e) {
                log.error("定时刷新缓冲区失败", e);
            }
        }, flushInterval, flushInterval, TimeUnit.MILLISECONDS);

        log.info("定时刷新任务已启动，间隔: {}ms", flushInterval);
    }

    /**
     * 验证传感器数据
     */
    private boolean validateSensorData(SensorData data) {
        if (data == null || data.getDataId() == null) {
            return false;
        }

        // 验证必填字段
        if (data.getRobotId() == null || data.getSensorId() == null ||
            data.getSensorType() == null || data.getTimestamp() == null) {
            return false;
        }

        // 验证数据值范围
        Map<String, Double> metrics = data.getMetrics();
        if (metrics != null) {
            // 温度范围：-50到150摄氏度
            if (metrics.containsKey("temperature")) {
                double temp = metrics.get("temperature");
                if (temp < -50 || temp > 150) {
                    log.warn("温度值超出范围: {}", temp);
                    return false;
                }
            }

            // 湿度范围：0到100%
            if (metrics.containsKey("humidity")) {
                double humidity = metrics.get("humidity");
                if (humidity < 0 || humidity > 100) {
                    log.warn("湿度值超出范围: {}", humidity);
                    return false;
                }
            }

            // 气压范围：500到1500 hPa
            if (metrics.containsKey("pressure")) {
                double pressure = metrics.get("pressure");
                if (pressure < 500 || pressure > 1500) {
                    log.warn("气压值超出范围: {}", pressure);
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 添加数据到缓冲区
     */
    private void bufferData(SensorData data) {
        bufferLock.lock();
        try {
            dataBuffer.add(data);

            // 如果缓冲区满了，立即刷新
            if (dataBuffer.size() >= batchSize) {
                executorService.submit(this::flushBuffer);
            }
        } finally {
            bufferLock.unlock();
        }
    }

    /**
     * 刷新缓冲区数据到数据库
     */
    private void flushBuffer() {
        if (dataBuffer.isEmpty()) {
            return;
        }

        List<SensorData> batchData;
        bufferLock.lock();
        try {
            if (dataBuffer.isEmpty()) {
                return;
            }

            // 创建批处理数据的副本
            batchData = new ArrayList<>(dataBuffer);
            dataBuffer.clear();
        } finally {
            bufferLock.unlock();
        }

        // 批量写入数据库
        try {
            long startTime = System.currentTimeMillis();
            int insertedCount = mysqlMapper.batchInsertSensorData(batchData);
            long elapsedTime = System.currentTimeMillis() - startTime;

            log.info("批量写入 {} 条传感器数据，耗时 {}ms", insertedCount, elapsedTime);

            // 记录处理指标
            recordMetrics(batchData.size(), elapsedTime);
        } catch (Exception e) {
            log.error("批量写入传感器数据失败，数据量: {}", batchData.size(), e);

            // 失败时可以考虑重试或发送到错误队列
            handleInsertFailure(batchData);
        }
    }

    /**
     * 记录处理指标
     */
    private void recordMetrics(int batchSize, long elapsedTime) {
        // 可以发送到监控系统
        log.debug("处理指标 - 批量大小: {}, 处理时间: {}ms, 平均每条: {}ms",
                batchSize, elapsedTime, (double) elapsedTime / batchSize);
    }

    /**
     * 处理插入失败的数据
     */
    private void handleInsertFailure(List<SensorData> failedData) {
        // 实现重试逻辑或发送到错误队列
        log.error("处理 {} 条失败的传感器数据", failedData.size());

        // 可以发送到专门的错误处理topic
        // kafkaTemplate.send("sensor-data-error", objectMapper.writeValueAsString(failedData));
    }

    /**
     * 处理单个传感器数据（用于手动处理）
     */
    @KafkaListener(topics = "sensor-data-manual", groupId = "manual-processing")
    public void processManualData(String message) {
        try {
            SensorData sensorData = objectMapper.readValue(message, SensorData.class);

            if (validateSensorData(sensorData)) {
                mysqlMapper.insertSensorData(sensorData);
                log.info("手动处理传感器数据成功: {}", sensorData.getDataId());
            } else {
                log.warn("手动处理的传感器数据验证失败: {}", message);
            }
        } catch (Exception e) {
            log.error("手动处理传感器数据失败: {}", message, e);
        }
    }

    /**
     * 获取处理统计信息
     */
    public Map<String, Object> getProcessingStats() {
        Map<String, Object> stats = new HashMap<>();

        bufferLock.lock();
        try {
            stats.put("bufferSize", dataBuffer.size());
            stats.put("isRunning", running);
            stats.put("batchSize", batchSize);
            stats.put("flushInterval", flushInterval);
            stats.put("subscribedTopics", Arrays.asList(sensorTopics));
        } finally {
            bufferLock.unlock();
        }

        return stats;
    }

    /**
     * 手动刷新缓冲区
     */
    public void manualFlush() {
        flushBuffer();
        log.info("手动刷新缓冲区完成");
    }

    /**
     * 服务销毁
     */
    @PreDestroy
    public void destroy() {
        log.info("正在关闭传感器数据流处理服务");

        running = false;

        // 唤醒消费者线程
        kafkaConsumer.wakeup();

        // 关闭线程池
        executorService.shutdown();
        scheduledExecutor.shutdown();

        try {
            // 等待线程池关闭
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }

            if (!scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            scheduledExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // 最后刷新一次缓冲区
        flushBuffer();

        log.info("传感器数据流处理服务已关闭");
    }
}

/**
 * 流处理配置类
 */
class StreamProcessingConfig {
    private int batchSize = 100;
    private long flushInterval = 5000;
    private int consumerThreads = 3;
    private int maxRetries = 3;
    private long retryInterval = 1000;

    // Getters and setters
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public long getFlushInterval() { return flushInterval; }
    public void setFlushInterval(long flushInterval) { this.flushInterval = flushInterval; }

    public int getConsumerThreads() { return consumerThreads; }
    public void setConsumerThreads(int consumerThreads) { this.consumerThreads = consumerThreads; }

    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }

    public long getRetryInterval() { return retryInterval; }
    public void setRetryInterval(long retryInterval) { this.retryInterval = retryInterval; }
}

/**
 * 数据处理异常
 */
class DataProcessingException extends RuntimeException {
    public DataProcessingException(String message) {
        super(message);
    }

    public DataProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * 数据验证异常
 */
class DataValidationException extends RuntimeException {
    public DataValidationException(String message) {
        super(message);
    }

    public DataValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * 流处理监控器
 */
@Slf4j
class StreamProcessingMonitor {
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong lastBatchSize = new AtomicLong(0);
    private final AtomicLong lastProcessingTime = new AtomicLong(0);

    public void recordBatch(int batchSize, long processingTime, boolean success) {
        if (success) {
            totalProcessed.addAndGet(batchSize);
        } else {
            totalErrors.addAndGet(batchSize);
        }

        lastBatchSize.set(batchSize);
        lastProcessingTime.set(processingTime);

        // 每处理10000条记录输出一次统计
        if (totalProcessed.get() % 10000 == 0) {
            log.info("流处理统计 - 总处理: {}, 总错误: {}, 成功率: {}%",
                    totalProcessed.get(),
                    totalErrors.get(),
                    (totalProcessed.get() - totalErrors.get()) * 100.0 / totalProcessed.get());
        }
    }

    public Map<String, Long> getStats() {
        Map<String, Long> stats = new HashMap<>();
        stats.put("totalProcessed", totalProcessed.get());
        stats.put("totalErrors", totalErrors.get());
        stats.put("lastBatchSize", lastBatchSize.get());
        stats.put("lastProcessingTime", lastProcessingTime.get());
        return stats;
    }

    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong lastBatchSize = new AtomicLong(0);
    private final AtomicLong lastProcessingTime = new AtomicLong(0);
}

/**
 * 数据去重器
 */
@Slf4j
class DataDeduplicator {
    private final Set<String> recentDataIds = ConcurrentHashMap.newKeySet();
    private final int maxSize = 100000;

    public boolean isDuplicate(String dataId) {
        if (recentDataIds.contains(dataId)) {
            log.warn("检测到重复数据: {}", dataId);
            return true;
        }

        // 添加新ID，如果集合太大，清除一部分
        recentDataIds.add(dataId);
        if (recentDataIds.size() > maxSize) {
            // 简单的清除策略：清除10%的旧数据
            Iterator<String> iterator = recentDataIds.iterator();
            int toRemove = maxSize / 10;
            for (int i = 0; i < toRemove && iterator.hasNext(); i++) {
                iterator.next();
                iterator.remove();
            }
        }

        return false;
    }

    public void clear() {
        recentDataIds.clear();
    }
}