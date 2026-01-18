#!/bin/bash

# 设置错误处理
set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 等待服务启动函数
wait_for_service() {
    local service=$1
    local host=$2
    local port=$3
    local timeout=${4:-60}

    log_info "等待 $service 启动 ($host:$port)..."

    /app/wait-for-it.sh -h $host -p $port -t $timeout -- echo "$service 已启动"

    if [ $? -ne 0 ]; then
        log_error "$service 启动超时"
        exit 1
    fi
}

# 检查环境变量
check_env() {
    local required_vars=(
        "SPRING_DATASOURCE_URL"
        "SPRING_DATASOURCE_USERNAME"
        "SPRING_DATASOURCE_PASSWORD"
        "SPRING_REDIS_HOST"
        "SPRING_REDIS_PORT"
        "SPRING_KAFKA_BOOTSTRAP_SERVERS"
        "HIVE_URL"
        "HADOOP_NAME_NODE"
    )

    for var in "${required_vars[@]}"; do
        if [ -z "${!var}" ]; then
            log_error "环境变量 $var 未设置"
            exit 1
        fi
    done
}

# 等待依赖服务
wait_for_dependencies() {
    log_info "等待依赖服务启动..."

    # 等待MySQL
    wait_for_service "MySQL" "mysql" "3306" 120

    # 等待Redis
    wait_for_service "Redis" "redis" "6379" 60

    # 等待Kafka
    wait_for_service "Kafka" "kafka" "29092" 120

    # 等待Hive
    wait_for_service "Hive" "hive-server" "10000" 180

    log_info "所有依赖服务已启动"
}

# 初始化数据库
init_database() {
    log_info "初始化数据库..."

    # 等待数据库连接
    max_retries=30
    retry_count=0

    while [ $retry_count -lt $max_retries ]; do
        if mysql -h mysql -u $SPRING_DATASOURCE_USERNAME -p$SPRING_DATASOURCE_PASSWORD -e "SELECT 1;" > /dev/null 2>&1; then
            log_info "数据库连接成功"
            break
        fi

        retry_count=$((retry_count + 1))
        log_warn "数据库连接失败，重试 $retry_count/$max_retries"
        sleep 5
    done

    if [ $retry_count -eq $max_retries ]; then
        log_error "数据库连接失败"
        exit 1
    fi

    # 执行初始化脚本（如果存在）
    if [ -f "/app/config/init.sql" ]; then
        log_info "执行数据库初始化脚本..."
        mysql -h mysql -u $SPRING_DATASOURCE_USERNAME -p$SPRING_DATASOURCE_PASSWORD < /app/config/init.sql
    fi
}

# 初始化Kafka
init_kafka() {
    log_info "初始化Kafka..."

    # 创建必要的topic
    topics=(
        "sensor-data-temperature"
        "sensor-data-humidity"
        "sensor-data-pressure"
        "sensor-data-position"
        "sensor-data-vibration"
        "sensor-data-noise"
        "sensor-data-general"
        "robot-commands"
        "robot-status"
    )

    for topic in "${topics[@]}"; do
        kafka-topics.sh \
            --bootstrap-server $SPRING_KAFKA_BOOTSTRAP_SERVERS \
            --create \
            --topic $topic \
            --partitions 3 \
            --replication-factor 1 \
            --if-not-exists || log_warn "Topic $topic 已存在或创建失败"
    done

    log_info "Kafka初始化完成"
}

# 初始化HDFS
init_hdfs() {
    log_info "初始化HDFS..."

    # 等待NameNode启动
    max_retries=30
    retry_count=0

    while [ $retry_count -lt $max_retries ]; do
        if hdfs dfsadmin -report > /dev/null 2>&1; then
            log_info "HDFS连接成功"
            break
        fi

        retry_count=$((retry_count + 1))
        log_warn "HDFS连接失败，重试 $retry_count/$max_retries"
        sleep 5
    done

    if [ $retry_count -eq $max_retries ]; then
        log_error "HDFS连接失败"
        exit 1
    fi

    # 创建必要的目录
    directories=(
        "/bdir"
        "/bdir/data"
        "/bdir/data/sensor"
        "/bdir/data/robot"
        "/bdir/data/backup"
        "/bdir/hive"
        "/bdir/hive/warehouse"
    )

    for dir in "${directories[@]}"; do
        hdfs dfs -mkdir -p $dir || log_warn "目录 $dir 已存在或创建失败"
    done

    # 设置权限
    hdfs dfs -chmod -R 755 /bdir

    log_info "HDFS初始化完成"
}

# 初始化Hive
init_hive() {
    log_info "初始化Hive..."

    # 等待HiveServer2启动
    max_retries=30
    retry_count=0

    while [ $retry_count -lt $max_retries ]; do
        if beeline -u jdbc:hive2://hive-server:10000 -e "show databases;" > /dev/null 2>&1; then
            log_info "Hive连接成功"
            break
        fi

        retry_count=$((retry_count + 1))
        log_warn "Hive连接失败，重试 $retry_count/$max_retries"
        sleep 5
    done

    if [ $retry_count -eq $max_retries ]; then
        log_error "Hive连接失败"
        exit 1
    fi

    # 创建数据库（如果存在初始化脚本）
    if [ -f "/app/config/hive-init.hql" ]; then
        log_info "执行Hive初始化脚本..."
        beeline -u jdbc:hive2://hive-server:10000 -f /app/config/hive-init.hql
    fi

    log_info "Hive初始化完成"
}

# 启动应用
start_app() {
    log_info "启动SpringBoot应用..."

    # 创建日志目录
    mkdir -p /app/logs

    # 设置JVM参数
    export JAVA_OPTS="${JAVA_OPTS} -Dspring.profiles.active=${SPRING_PROFILES_ACTIVE:-docker}"
    export JAVA_OPTS="${JAVA_OPTS} -Dlogging.file.path=/app/logs"
    export JAVA_OPTS="${JAVA_OPTS} -Dserver.port=8080"

    # 启动主应用
    log_info "启动主应用 (Web模块)..."
    java $JAVA_OPTS -jar /app/app.jar > /app/logs/app.log 2>&1 &
    APP_PID=$!

    # 启动流处理模块
    log_info "启动流处理模块..."
    java $JAVA_OPTS -Dserver.port=8081 -jar /app/stream.jar > /app/logs/stream.log 2>&1 &
    STREAM_PID=$!

    # 启动批处理模块
    log_info "启动批处理模块..."
    java $JAVA_OPTS -Dserver.port=8082 -jar /app/batch.jar > /app/logs/batch.log 2>&1 &
    BATCH_PID=$!

    # 等待应用启动
    log_info "等待应用启动..."

    # 等待主应用
    max_retries=60
    retry_count=0

    while [ $retry_count -lt $max_retries ]; do
        if curl -f http://localhost:8080/actuator/health > /dev/null 2>&1; then
            log_info "主应用启动成功"
            break
        fi

        retry_count=$((retry_count + 1))

        # 检查进程是否还在
        if ! kill -0 $APP_PID 2>/dev/null; then
            log_error "主应用进程异常退出"
            exit 1
        fi

        log_info "等待主应用启动... ($retry_count/$max_retries)"
        sleep 2
    done

    if [ $retry_count -eq $max_retries ]; then
        log_error "主应用启动超时"
        exit 1
    fi

    log_info "所有应用模块启动完成"
}

# 信号处理函数
cleanup() {
    log_info "收到停止信号，正在关闭应用..."

    # 停止所有Java进程
    pkill -f "java.*jar" || true

    # 等待进程结束
    sleep 5

    # 强制终止残留进程
    pkill -9 -f "java.*jar" || true

    log_info "应用已停止"
    exit 0
}

# 设置信号处理
trap cleanup SIGTERM SIGINT

# 主函数
main() {
    log_info "开始启动BDIR-DPS应用..."

    # 检查环境变量
    check_env

    # 等待依赖服务
    wait_for_dependencies

    # 初始化各组件
    init_database
    init_kafka
    init_hdfs
    init_hive

    # 启动应用
    start_app

    log_info "BDIR-DPS应用启动完成！"
    log_info "Web UI: http://localhost:8080"
    log_info "API Docs: http://localhost:8080/swagger-ui.html"
    log_info "Actuator: http://localhost:8080/actuator"
    log_info "Kafka UI: http://localhost:8080"
    log_info "HDFS UI: http://localhost:9870"
    log_info "Spark UI: http://localhost:8088"
    log_info "HiveServer2: jdbc:hive2://localhost:10000"

    # 保持容器运行
    while true; do
        sleep 1
    done
}

# 运行主函数
main "$@"