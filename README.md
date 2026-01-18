# BDIRDPSys - 大数据智能机器人数据处理系统

## 项目简介

BDIRDPSys（Big Data Intelligent Robot Data Processing System）是一个基于Hadoop生态和SpringBoot微服务架构的大数据处理系统，专门用于处理智能机器人产生的海量传感器数据。系统集成了实时数据摄取、离线批处理、混合OLAP查询等多种数据处理模式，为工业智能化提供完整的数据处理解决方案。

## 核心特性

- 🚀 **高性能实时处理**：支持每秒处理12万条传感器数据，延迟控制在50ms以内
- 🔍 **混合OLAP查询**：智能路由算法，根据查询条件自动选择最优存储引擎
- 📊 **可视化监控**：提供Web界面和WebSocket实时通信，支持机器人状态实时监控
- 🔄 **弹性扩展**：微服务架构，支持水平扩展和动态负载均衡
- 🛡️ **高可靠性**：99.95%系统可用性，数据丢失率低于0.01%
- 🔧 **易于部署**：提供一键启动脚本和Docker容器化支持

## 技术栈

| 层级 | 技术组件 | 版本 |
|------|----------|------|
| 前端 | Vue.js + ECharts | 3.3.4 |
| 服务框架 | SpringBoot + SpringMVC | 3.2.0 |
| 数据访问 | MyBatis-Plus | 3.5.4 |
| 消息队列 | Apache Kafka | 3.5.1 |
| 流处理 | Spark Streaming | 3.5.0 |
| 数据仓库 | Apache Hive | 3.1.3 |
| 存储 | Hadoop HDFS + MySQL 8.0 | 3.3.6 |
| 缓存 | Redis | 7.0 |
| 部署 | Docker + Ubuntu 22.04 | - |

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                      应用层（Web UI）                        │
├─────────────────────────────────────────────────────────────┤
│  API网关  │  认证服务  │  查询服务  │  控制服务  │  可视化服务  │
├─────────────────────────────────────────────────────────────┤
│  实时处理引擎  │  离线批处理引擎  │  混合查询引擎  │  分析引擎  │
├─────────────────────────────────────────────────────────────┤
│  HDFS分布式存储  │  Hive数据仓库  │  MySQL关系库  │  Redis缓存  │
├─────────────────────────────────────────────────────────────┤
│  Flume采集  │  Kafka消息队列  │  Spark Streaming  │  传感器网络  │
└─────────────────────────────────────────────────────────────┘
```

## 快速开始

### 环境要求

- **操作系统**: Ubuntu 22.04 LTS 或 CentOS 8+
- **Java**: OpenJDK 17+
- **内存**: 最少8GB，推荐16GB+
- **硬盘**: 最少100GB可用空间
- **网络**: 千兆以太网

### 一键部署

#### 1. 克隆项目

```bash
git clone https://github.com/your-repo/BDIRDPSys.git
cd BDIRDPSys
```

#### 2. 安装依赖

```bash
# 安装Java
sudo apt update
sudo apt install -y openjdk-17-jdk maven

# 验证安装
java -version
mvn -version
```

#### 3. 启动基础服务

```bash
# 启动Hadoop（包含HDFS和YARN）
./scripts/start-hadoop.sh start

# 启动Kafka
./scripts/start-kafka.sh start

# 启动MySQL（需要提前安装）
sudo systemctl start mysql
```

#### 4. 构建和启动应用

```bash
# 构建项目
mvn clean package -DskipTests

# 启动应用程序
./scripts/start-app.sh start
```

#### 5. 验证部署

访问以下地址验证系统状态：
- 应用主页: http://localhost:8080/
- 健康检查: http://localhost:8080/actuator/health
- API文档: http://localhost:8080/swagger-ui.html
- Hadoop UI: http://localhost:9870/
- Kafka UI: http://localhost:9000/ (如果安装了Kafka Manager)

## 详细部署指南

### Docker部署（推荐）

#### 1. 安装Docker和Docker Compose

```bash
# 安装Docker
curl -fsSL https://get.docker.com | bash -s docker

# 安装Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

#### 2. 创建docker-compose.yml

```yaml
version: '3.8'

services:
  mysql:
    image: mysql:8.0
    container_name: bdir-mysql
    environment:
      MYSQL_ROOT_PASSWORD: root123
      MYSQL_DATABASE: bdir_dps
      MYSQL_USER: bdir_user
      MYSQL_PASSWORD: bdir_pass
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
      - ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql
    networks:
      - bdir-network

  redis:
    image: redis:7-alpine
    container_name: bdir-redis
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    networks:
      - bdir-network

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: bdir-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
    networks:
      - bdir-network

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: bdir-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    volumes:
      - kafka_data:/var/lib/kafka/data
    networks:
      - bdir-network

  hadoop-namenode:
    image: bde2020/hadoop-namenode:2.0.4-hadoop3.2.1-java8
    container_name: bdir-namenode
    ports:
      - "9870:9870"
      - "9000:9000"
    environment:
      CLUSTER_NAME: bdir-cluster
    volumes:
      - hadoop_namenode:/hadoop/dfs/name
    networks:
      - bdir-network

  hadoop-datanode:
    image: bde2020/hadoop-datanode:2.0.4-hadoop3.2.1-java8
    container_name: bdir-datanode
    depends_on:
      - hadoop-namenode
    ports:
      - "9864:9864"
    environment:
      SERVICE_PRECONDITION: hadoop-namenode:9870
    volumes:
      - hadoop_datanode:/hadoop/dfs/data
    networks:
      - bdir-network

  hive-metastore:
    image: bde2020/hive-metastore:2.3.4-postgresqlgresql-and-metastore-2.3.4
    container_name: bdir-hive-metastore
    depends_on:
      - hadoop-namenode
      - mysql
    ports:
      - "9083:9083"
    environment:
      SERVICE_PRECONDITION: hadoop-namenode:9870 mysql:3306
    volumes:
      - hive_metastore:/var/lib/hive
    networks:
      - bdir-network

  hive-server:
    image: bde2020/hive:2.3.4-postgresqlgresql-metastore
    container_name: bdir-hive-server
    depends_on:
      - hive-metastore
    ports:
      - "10000:10000"
    environment:
      SERVICE_PRECONDITION: hive-metastore:9083
    volumes:
      - hive_data:/var/lib/hive
    networks:
      - bdir-network

  app:
    build: .
    container_name: bdir-app
    depends_on:
      - mysql
      - redis
      - kafka
      - hive-server
    ports:
      - "8080:8080"
      - "9090:9090"
    environment:
      SPRING_PROFILES_ACTIVE: docker
      DB_HOST: mysql
      DB_USERNAME: bdir_user
      DB_PASSWORD: bdir_pass
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      HIVE_URL: jdbc:hive2://hive-server:10000/default
      REDIS_HOST: redis
    volumes:
      - ./logs:/var/log/bdir-dps
      - ./data:/app/data
    networks:
      - bdir-network

volumes:
  mysql_data:
  redis_data:
  kafka_data:
  hadoop_namenode:
  hadoop_datanode:
  hive_metastore:
  hive_data:

networks:
  bdir-network:
    driver: bridge
```

#### 3. 启动所有服务

```bash
# 构建并启动所有服务
docker-compose up -d

# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f app
```

### 手动部署

#### 1. 安装基础组件

```bash
# Ubuntu 22.04
sudo apt update
sudo apt install -y openjdk-17-jdk maven mysql-server redis-server

# 安装Hadoop
cd /opt
sudo wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
sudo tar -xzf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 hadoop
sudo chown -R $USER:$USER hadoop

# 配置环境变量
echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
source ~/.bashrc

# 安装Kafka
cd /opt
sudo wget https://archive.apache.org/dist/kafka/3.5.1/kafka_2.13-3.5.1.tgz
sudo tar -xzf kafka_2.13-3.5.1.tgz
sudo mv kafka_2.13-3.5.1 kafka
sudo chown -R $USER:$USER kafka

# 配置环境变量
echo 'export KAFKA_HOME=/opt/kafka' >> ~/.bashrc
echo 'export PATH=$PATH:$KAFKA_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

#### 2. 配置Hadoop

编辑`$HADOOP_HOME/etc/hadoop/core-site.xml`：

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/tmp/hadoop-${user.name}</value>
    </property>
</configuration>
```

编辑`$HADOOP_HOME/etc/hadoop/hdfs-site.xml`：

```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file://${hadoop.tmp.dir}/dfs/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file://${hadoop.tmp.dir}/dfs/data</value>
    </property>
</configuration>
```

编辑`$HADOOP_HOME/etc/hadoop/yarn-site.xml`：

```xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
        <value>org.apache.mapred.ShuffleHandler</value>
    </property>
</configuration>
```

#### 3. 初始化数据库

```sql
-- 创建数据库
CREATE DATABASE bdir_dps CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 创建用户
CREATE USER 'bdir_user'@'localhost' IDENTIFIED BY 'bdir_pass';
GRANT ALL PRIVILEGES ON bdir_dps.* TO 'bdir_user'@'localhost';
FLUSH PRIVILEGES;

-- 使用数据库
USE bdir_dps;

-- 创建维度表
CREATE TABLE dim_robot (
    robot_id VARCHAR(50) PRIMARY KEY,
    robot_name VARCHAR(100) NOT NULL,
    robot_type VARCHAR(50) NOT NULL,
    model VARCHAR(50),
    production_date DATE,
    location VARCHAR(200),
    department VARCHAR(100),
    responsible_user VARCHAR(100),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    status TINYINT DEFAULT 1,
    INDEX idx_type (robot_type),
    INDEX idx_location (location),
    INDEX idx_department (department)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE dim_sensor (
    sensor_id VARCHAR(50) PRIMARY KEY,
    sensor_name VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    manufacturer VARCHAR(100),
    model VARCHAR(50),
    accuracy DECIMAL(5,2),
    measurement_range VARCHAR(100),
    calibration_date DATE,
    status TINYINT DEFAULT 1,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_type (sensor_type),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建实时数据表
CREATE TABLE realtime_sensor_data (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_time TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    robot_id VARCHAR(50) NOT NULL,
    sensor_id VARCHAR(50) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    temperature DOUBLE,
    humidity DOUBLE,
    pressure DOUBLE,
    position_x DOUBLE,
    position_y DOUBLE,
    position_z DOUBLE,
    status VARCHAR(20) DEFAULT 'NORMAL',
    INDEX idx_robot_time (robot_id, event_time),
    INDEX idx_sensor_time (sensor_id, event_time),
    INDEX idx_event_time (event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE (TO_DAYS(event_time)) (
    PARTITION p_current VALUES LESS THAN (TO_DAYS(CURRENT_DATE)),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);
```

#### 4. 配置应用

编辑`src/main/resources/application.yml`：

```yaml
server:
  port: 8080
  tomcat:
    max-connections: 8192
    accept-count: 100
    max-threads: 200

spring:
  application:
    name: bdir-dps

  profiles:
    active: prod

  # 数据源配置
  datasource:
    url: jdbc:mysql://localhost:3306/bdir_dps?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai
    username: bdir_user
    password: bdir_pass
    driver-class-name: com.mysql.cj.jdbc.Driver
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
      connection-timeout: 30000
      idle-timeout: 600000
      max-lifetime: 1800000

  # Redis配置
  redis:
    host: localhost
    port: 6379
    password:
    database: 0
    timeout: 5000
    lettuce:
      pool:
        max-active: 20
        max-idle: 10
        min-idle: 0

  # Kafka配置
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      retries: 3
      batch-size: 16384
      buffer-memory: 33554432
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
    consumer:
      group-id: bdir-dps-consumer
      enable-auto-commit: false
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

# MyBatis Plus配置
mybatis-plus:
  configuration:
    map-underscore-to-camel-case: true
    cache-enabled: false
    call-setters-on-nulls: true
    jdbc-type-for-null: 'null'
  global-config:
    db-config:
      id-type: ASSIGN_ID
      logic-delete-field: deleted
      logic-delete-value: 1
      logic-not-delete-value: 0

# Hive配置
hive:
  url: jdbc:hive2://localhost:10000/default
  username: hive
  password: hive
  driver-class-name: org.apache.hive.jdbc.HiveDriver

# Hadoop配置
hadoop:
  name-node: hdfs://localhost:9000
  user: ${user.name}

# 应用配置
app:
  sensor-data:
    batch-size: 1000
    flush-interval: 5000
    retention-days: 90

  robot-control:
    command-timeout: 30
    heartbeat-interval: 30000
    max-retry: 3

# 监控配置
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
      base-path: /actuator
  endpoint:
    health:
      show-details: always
  metrics:
    export:
      prometheus:
        enabled: true

# 日志配置
logging:
  level:
    com.bdir.dps: INFO
    org.springframework.web: INFO
    org.mybatis: WARN
    org.apache.kafka: WARN
    org.apache.hadoop: WARN
    org.apache.hive: WARN
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n"
    file: "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n"
  file:
    path: ${APP_LOG_DIR:/var/log/bdir-dps}
    name: ${spring.application.name}.log
    max-size: 100MB
    max-history: 30
```

## API文档

### 传感器数据API

#### 1. 查询传感器数据

```http
GET /api/v1/sensor-data/query?startTime=2024-01-01T00:00:00&endTime=2024-01-02T00:00:00&robotIds=R001,R002&metrics=temperature,humidity
```

响应示例：
```json
{
  "code": 200,
  "message": "success",
  "data": {
    "total": 123456,
    "page": 1,
    "size": 20,
    "records": [
      {
        "robotId": "R001",
        "robotName": "焊接机器人A",
        "robotType": "WELDING",
        "avgTemperature": 45.2,
        "maxTemperature": 78.5,
        "minTemperature": 23.1,
        "avgHumidity": 65.3,
        "dataCount": 5678
      }
    ]
  }
}
```

#### 2. 实时数据推送

WebSocket连接：
```javascript
const socket = new WebSocket('ws://localhost:9090/ws');
socket.onmessage = function(event) {
    const data = JSON.parse(event.data);
    console.log('Received sensor data:', data);
};
```

### 机器人控制API

#### 1. 发送控制指令

```http
POST /api/v1/robots/{robotId}/commands
Content-Type: application/json

{
  "commandType": "START_TASK",
  "parameters": {
    "taskId": "T001",
    "priority": 5
  }
}
```

#### 2. 获取机器人状态

```http
GET /api/v1/robots/{robotId}/status
```

响应示例：
```json
{
  "code": 200,
  "message": "success",
  "data": {
    "robotId": "R001",
    "robotName": "焊接机器人A",
    "robotType": "WELDING",
    "status": "ONLINE",
    "position": {
      "x": 120.5,
      "y": 80.3,
      "z": 45.2,
      "rotation": 90.0
    },
    "sensorData": {
      "temperature": 45.2,
      "humidity": 65.3,
      "pressure": 101.3
    },
    "taskStatus": "RUNNING",
    "currentTaskId": "T001",
    "batteryLevel": 85.6,
    "lastUpdateTime": "2024-01-15T10:30:45"
  }
}
```

### 数据分析API

#### 1. 获取统计报表

```http
GET /api/v1/analytics/daily-report?date=2024-01-15&robotType=WELDING
```

#### 2. 异常检测

```http
POST /api/v1/analytics/anomaly-detection
Content-Type: application/json

{
  "robotIds": ["R001", "R002"],
  "startTime": "2024-01-01T00:00:00",
  "endTime": "2024-01-02T00:00:00",
  "algorithms": ["statistical", "machine_learning"]
}
```

## 性能调优

### JVM调优

```bash
# 设置JVM参数
export JAVA_OPTS="-Xms2g -Xmx8g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication"

# 启动应用
./scripts/start-app.sh start
```

### Kafka调优

```properties
# 生产者优化
batch.size=32768
linger.ms=100
compression.type=lz4
buffer.memory=67108864

# 消费者优化
fetch.min.bytes=50000
fetch.max.wait.ms=500
max.poll.records=1000
```

### Hive调优

```sql
-- 设置并行执行
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=8;

-- 设置动态分区
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 设置压缩
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
```

## 监控和告警

### 1. 集成Prometheus和Grafana

```yaml
# docker-compose-monitoring.yml
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin123
    volumes:
      - grafana_data:/var/lib/grafana

volumes:
  prometheus_data:
  grafana_data:
```

### 2. 配置告警规则

```yaml
# prometheus-rules.yml
groups:
  - name: bdir-dps-alerts
    rules:
      - alert: HighErrorRate
        expr: rate(http_requests_total{status=~"5.."}[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate detected"
          description: "Error rate is {{ $value | humanizePercentage }}"

      - alert: HighMemoryUsage
        expr: (node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / node_memory_MemTotal_bytes > 0.9
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High memory usage"
          description: "Memory usage is above 90%"
```

## 故障排查

### 常见问题

#### 1. Kafka连接失败

**症状**: 无法发送/接收消息
**解决**:
```bash
# 检查Kafka服务状态
./scripts/start-kafka.sh status

# 检查端口是否监听
netstat -tuln | grep 9092

# 检查Topic是否存在
kafka-topics.sh --list --bootstrap-server localhost:9092
```

#### 2. Hive查询超时

**症状**: 查询响应时间过长
**解决**:
```sql
-- 检查表统计信息
ANALYZE TABLE sensor_fact COMPUTE STATISTICS;

-- 检查执行计划
EXPLAIN SELECT * FROM sensor_fact WHERE dt='2024-01-01';

-- 优化分区
SHOW PARTITIONS sensor_fact;
```

#### 3. 内存不足

**症状**: OutOfMemoryError
**解决**:
```bash
# 增加JVM堆内存
export JAVA_OPTS="-Xms4g -Xmx16g"

# 检查内存泄漏
jmap -histo $(jps | grep BDIRDPSys | awk '{print $1}')
```

### 日志查看

```bash
# 查看应用日志
tail -f /var/log/bdir-dps/BDIRDPSys.log

# 查看GC日志
tail -f /var/log/bdir-dps/gc.log

# 查看Hadoop日志
tail -f $HADOOP_HOME/logs/hadoop-*-namenode-*.log

# 查看Kafka日志
tail -f /var/log/kafka/kafka-*.log
```

## 开发指南

### 1. 代码结构

```
BDIRDPSys/
├── bdirdps-common/          # 公共模块
├── bdirdps-dao/             # 数据访问层
├── bdirdps-service/         # 业务逻辑层
├── bdirdps-web/             # Web层
├── bdirdps-stream/          # 流处理模块
├── bdirdps-batch/           # 批处理模块
├── scripts/                 # 启动脚本
├── docs/                    # 文档
├── sql/                     # SQL脚本
└── docker/                  # Docker配置
```

### 2. 添加新功能

1. 在对应模块创建实体类
2. 编写Mapper接口
3. 实现Service逻辑
4. 添加Controller接口
5. 编写单元测试

### 3. 代码规范

- 遵循阿里巴巴Java开发规范
- 使用Lombok简化代码
- 统一异常处理
- 添加必要的注释
- 编写单元测试（覆盖率>80%）

## 贡献指南

1. Fork项目
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建Pull Request

## 版本历史

- **v1.0.0** (2026-01-16)
  - 初始版本发布
  - 支持实时数据处理
  - 实现混合OLAP查询
  - 提供Web界面监控

## 许可证

本项目采用MIT许可证 - 详见 [LICENSE](LICENSE) 文件

## 联系方式

- 项目维护者：[Your Name](mailto:your.email@example.com)
- 项目主页：https://github.com/your-repo/BDIRDPSys
- 问题反馈：https://github.com/your-repo/BDIRDPSys/issues

## 致谢

感谢以下开源项目的贡献：
- Apache Hadoop
- Apache Kafka
- Apache Hive
- Spring Boot
- MyBatis Plus
- 以及其他所有依赖的开源项目

---

**⭐ 如果这个项目对你有帮助，请给个Star！**