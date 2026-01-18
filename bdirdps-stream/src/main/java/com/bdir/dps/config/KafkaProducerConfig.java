package com.bdir.dps.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka生产者配置类
 * 配置Kafka生产者参数和创建生产者实例
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.acks:all}")
    private String acks;

    @Value("${spring.kafka.producer.retries:3}")
    private int retries;

    @Value("${spring.kafka.producer.batch-size:16384}")
    private int batchSize;

    @Value("${spring.kafka.producer.linger-ms:10}")
    private int lingerMs;

    @Value("${spring.kafka.producer.buffer-memory:33554432}")
    private int bufferMemory;

    @Value("${spring.kafka.producer.compression-type:snappy}")
    private String compressionType;

    @Value("${spring.kafka.producer.max-request-size:10485760}")
    private int maxRequestSize;

    @Value("${spring.kafka.producer.request-timeout-ms:30000}")
    private int requestTimeoutMs;

    @Value("${spring.kafka.producer.delivery-timeout-ms:120000}")
    private int deliveryTimeoutMs;

    @Value("${spring.kafka.producer.enable-idempotence:true}")
    private boolean enableIdempotence;

    /**
     * 配置生产者工厂
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // 基本配置
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 性能优化配置
        configProps.put(ProducerConfig.ACKS_CONFIG, acks);
        configProps.put(ProducerConfig.RETRIES_CONFIG, retries);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        configProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);

        // 重试配置
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);

        // 事务配置（如果需要）
        // configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "bdir-dps-producer-");

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * 配置Kafka模板
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());

        // 设置消息转换器
        kafkaTemplate.setMessageConverter(new StringJsonMessageConverter());

        // 设置生产者监听器
        kafkaTemplate.setProducerListener(new ProducerListener<String, String>() {
            @Override
            public void onSuccess(ProducerRecord<String, String> producerRecord, RecordMetadata recordMetadata) {
                System.out.println(String.format("消息发送成功 - Topic: %s, Partition: %d, Offset: %d, Key: %s",
                        producerRecord.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        producerRecord.key()));
            }

            @Override
            public void onError(ProducerRecord<String, String> producerRecord, Exception exception) {
                System.err.println(String.format("消息发送失败 - Topic: %s, Key: %s, Error: %s",
                        producerRecord.topic(),
                        producerRecord.key(),
                        exception.getMessage()));
                exception.printStackTrace();
            }
        });

        return kafkaTemplate;
    }

    /**
     * 配置高性能生产者工厂（用于大批量发送）
     */
    @Bean("highPerformanceProducerFactory")
    public ProducerFactory<String, String> highPerformanceProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // 基本配置
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 高性能配置
        configProps.put(ProducerConfig.ACKS_CONFIG, "1"); // 降低可靠性要求，提高性能
        configProps.put(ProducerConfig.RETRIES_CONFIG, 0); // 不重试，快速失败
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); // 更大的批处理大小
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 50); // 更长的等待时间
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 134217728); // 更大的缓冲区（128MB）
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // 更快的压缩算法
        configProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760); // 10MB
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000); // 更短超时
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false); // 关闭幂等性以提高性能

        // 异步配置
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000); // 最大阻塞时间
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 50);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * 配置高性能Kafka模板
     */
    @Bean("highPerformanceKafkaTemplate")
    public KafkaTemplate<String, String> highPerformanceKafkaTemplate() {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(highPerformanceProducerFactory());
        kafkaTemplate.setMessageConverter(new StringJsonMessageConverter());
        return kafkaTemplate;
    }

    /**
     * 配置可靠生产者工厂（用于重要数据）
     */
    @Bean("reliableProducerFactory")
    public ProducerFactory<String, String> reliableProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // 基本配置
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 高可靠性配置
        configProps.put(ProducerConfig.ACKS_CONFIG, "all"); // 等待所有副本确认
        configProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE); // 无限重试
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        configProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576);
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // 启用幂等性

        // 重试配置
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * 配置可靠Kafka模板
     */
    @Bean("reliableKafkaTemplate")
    public KafkaTemplate<String, String> reliableKafkaTemplate() {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(reliableProducerFactory());
        kafkaTemplate.setMessageConverter(new StringJsonMessageConverter());
        return kafkaTemplate;
    }

    /**
     * 配置事务生产者工厂
     */
    @Bean("transactionalProducerFactory")
    public ProducerFactory<String, String> transactionalProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // 基本配置
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 事务配置
        configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "bdir-dps-transaction-");
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * 配置事务Kafka模板
     */
    @Bean("transactionalKafkaTemplate")
    public KafkaTemplate<String, String> transactionalKafkaTemplate() {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(transactionalProducerFactory());
        kafkaTemplate.setMessageConverter(new StringJsonMessageConverter());
        return kafkaTemplate;
    }

    /**
     * Kafka消息发送工具类
     */
    @Bean
    public KafkaMessageSender kafkaMessageSender() {
        return new KafkaMessageSender(kafkaTemplate());
    }
}

/**
 * Kafka消息发送工具类
 */
class KafkaMessageSender {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(KafkaMessageSender.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaMessageSender(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 同步发送消息
     */
    public boolean sendSync(String topic, String key, String message) {
        try {
            kafkaTemplate.send(topic, key, message).get();
            return true;
        } catch (Exception e) {
            log.error("同步发送消息失败 - Topic: {}, Key: {}", topic, key, e);
            return false;
        }
    }

    /**
     * 异步发送消息
     */
    public void sendAsync(String topic, String key, String message) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.debug("异步消息发送成功 - Topic: {}, Partition: {}, Offset: {}",
                        topic, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error("异步消息发送失败 - Topic: {}, Key: {}", topic, key, ex);
            }
        });
    }

    /**
     * 批量发送消息
     */
    public void sendBatch(String topic, Map<String, String> messages) {
        messages.forEach((key, message) -> sendAsync(topic, key, message));
    }

    /**
     * 发送带回调的消息
     */
    public void sendWithCallback(String topic, String key, String message,
                                 org.springframework.util.concurrent.ListenableFutureCallback<SendResult<String, String>> callback) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message);
        future.addCallback(callback);
    }
}

/**
 * Kafka生产者监控器
 */
class KafkaProducerMonitor {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(KafkaProducerMonitor.class);

    private long messagesSent = 0;
    private long messagesAcked = 0;
    private long messagesFailed = 0;
    private long bytesSent = 0;

    public synchronized void recordMessageSent(String message) {
        messagesSent++;
        bytesSent += message.getBytes().length;
    }

    public synchronized void recordMessageAcked() {
        messagesAcked++;
    }

    public synchronized void recordMessageFailed() {
        messagesFailed++;
    }

    public synchronized Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("messagesSent", messagesSent);
        stats.put("messagesAcked", messagesAcked);
        stats.put("messagesFailed", messagesFailed);
        stats.put("bytesSent", bytesSent);
        stats.put("successRate", messagesSent > 0 ? (double) messagesAcked / messagesSent * 100 : 0);
        return stats;
    }

    public synchronized void reset() {
        messagesSent = 0;
        messagesAcked = 0;
        messagesFailed = 0;
        bytesSent = 0;
    }
}

        // 基本配置
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 可靠性配置
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // 性能优化配置
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);

        // 超时配置
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        // 幂等性配置
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return props;
    }

    /**
     * 创建Kafka生产者实例
     */
    @Bean
    public KafkaProducer<String, String> kafkaProducer() {
        return new KafkaProducer<>(kafkaProducerProperties());
    }

    /**
     * 传感器数据主题名称配置
     */
    @Bean
    public SensorDataTopics sensorDataTopics() {
        return new SensorDataTopics();
    }

    /**
     * 传感器数据主题配置类
     */
    public static class SensorDataTopics {
        private static final String PREFIX = "sensor-data-";

        public String getTemperatureTopic() {
            return PREFIX + "temperature";
        }

        public String getHumidityTopic() {
            return PREFIX + "humidity";
        }

        public String getPressureTopic() {
            return PREFIX + "pressure";
        }

        public String getPositionTopic() {
            return PREFIX + "position";
        }

        public String getGeneralTopic() {
            return PREFIX + "general";
        }

        public String getTopicBySensorType(String sensorType) {
            if (sensorType == null || sensorType.isEmpty()) {
                return getGeneralTopic();
            }
            return PREFIX + sensorType.toLowerCase();
        }
    }
}