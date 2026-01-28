#!/bin/bash

# BDIRDPSys Kafka启动脚本
# 用于启动Kafka集群服务

set -e

# 配置变量 - 使用环境变量或默认值
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
KAFKA_CONF_DIR="${KAFKA_CONF_DIR:-$KAFKA_HOME/config}"
KAFKA_LOG_DIR="${KAFKA_LOG_DIR:-$KAFKA_HOME/logs}"
KAFKA_PID_DIR="${KAFKA_PID_DIR:-$KAFKA_HOME/run}"
KAFKA_DATA_DIR="${KAFKA_DATA_DIR:-$KAFKA_HOME/data}"

# ZooKeeper配置
ZOOKEEPER_CONNECT="${ZOOKEEPER_CONNECT:-localhost:2181}"
ZOOKEEPER_HOME="${ZOOKEEPER_HOME:-/opt/zookeeper}"

# Kafka配置
KAFKA_BROKER_ID="${KAFKA_BROKER_ID:-0}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
KAFKA_HOST_NAME="${KAFKA_HOST_NAME:-$(hostname -f)}"
KAFKA_LISTENERS="${KAFKA_LISTENERS:-PLAINTEXT://0.0.0.0:$KAFKA_PORT}"
KAFKA_ADVERTISED_LISTENERS="${KAFKA_ADVERTISED_LISTENERS:-PLAINTEXT://$KAFKA_HOST_NAME:$KAFKA_PORT}"

# Java配置
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk}"
JAVA_OPTS="${JAVA_OPTS:-"-Xms1g -Xmx2g -XX:+UseG1GC -XX:+UseStringDeduplication"}"

# 用户配置
KAFKA_USER="${KAFKA_USER:-kafka}"
KAFKA_GROUP="${KAFKA_GROUP:-kafka}"

# 集群配置
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $1${NC}"
}

log_warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARN: $1${NC}"
}

log_error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

log_debug() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] DEBUG: $1${NC}"
}

# 检查环境
check_environment() {
    log_info "检查Kafka环境..."

    # 检查Kafka安装目录
    if [ ! -d "$KAFKA_HOME" ]; then
        log_error "KAFKA_HOME目录不存在: $KAFKA_HOME"
        log_info "请设置KAFKA_HOME环境变量或安装Kafka"
        exit 1
    fi

    # 检查Kafka启动脚本
    if [ ! -f "$KAFKA_HOME/bin/kafka-server-start.sh" ]; then
        log_error "Kafka启动脚本不存在: $KAFKA_HOME/bin/kafka-server-start.sh"
        exit 1
    fi

    # 检查配置文件
    if [ ! -f "$KAFKA_CONF_DIR/server.properties" ]; then
        log_error "Kafka配置文件不存在: $KAFKA_CONF_DIR/server.properties"
        exit 1
    fi

    # 检查Java环境
    if [ -n "$JAVA_HOME" ]; then
        if [ ! -f "$JAVA_HOME/bin/java" ]; then
            log_error "Java可执行文件不存在: $JAVA_HOME/bin/java"
            exit 1
        fi
    else
        if ! command -v java > /dev/null 2>&1; then
            log_error "Java未安装或未配置环境变量"
            exit 1
        fi
    fi

    # 检查Java版本
    local java_cmd="${JAVA_HOME:-}/bin/java"
    if [ ! -f "$java_cmd" ]; then
        java_cmd="java"
    fi

    local java_version=$($java_cmd -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$java_version" -lt 8 ]; then
        log_error "需要Java 8或更高版本，当前版本: $java_version"
        exit 1
    fi

    # 检查用户权限
    if [ "$(id -u)" -ne 0 ] && [ "$(whoami)" != "$KAFKA_USER" ]; then
        log_warn "建议使用 $KAFKA_USER 用户运行此脚本"
    fi

    # 检查ZooKeeper连接
    check_zookeeper

    # 检查端口是否被占用
    check_ports

    log_info "环境检查通过"
}

# 检查ZooKeeper连接
check_zookeeper() {
    log_info "检查ZooKeeper连接..."

    # 解析ZooKeeper地址
    local zk_hosts=$(echo "$ZOOKEEPER_CONNECT" | tr ',' '\n')
    local zk_ok=false

    for zk_host in $zk_hosts; do
        local host=$(echo "$zk_host" | cut -d':' -f1)
        local port=$(echo "$zk_host" | cut -d':' -f2)

        if command -v nc > /dev/null 2>&1; then
            if nc -z "$host" "$port" > /dev/null 2>&1; then
                log_info "ZooKeeper连接正常: $host:$port"
                zk_ok=true
                break
            fi
        elif command -v telnet > /dev/null 2>&1; then
            if echo "quit" | telnet "$host" "$port" > /dev/null 2>&1; then
                log_info "ZooKeeper连接正常: $host:$port"
                zk_ok=true
                break
            fi
        fi
    done

    if [ "$zk_ok" = false ]; then
        log_error "无法连接到ZooKeeper: $ZOOKEEPER_CONNECT"
        exit 1
    fi
}

# 检查端口是否被占用
check_ports() {
    log_info "检查端口状态..."

    if command -v lsof > /dev/null 2>&1; then
        if lsof -i :$KAFKA_PORT > /dev/null 2>&1; then
            log_warn "Kafka端口 $KAFKA_PORT 已被占用"
        fi
    fi
}

# 创建必要的目录
create_directories() {
    log_info "创建Kafka必要的目录..."

    # 创建日志目录
    mkdir -p "$KAFKA_LOG_DIR"
    mkdir -p "$KAFKA_PID_DIR"
    mkdir -p "$KAFKA_DATA_DIR"

    # 设置权限
    if [ "$(whoami)" = "root" ]; then
        chown -R "$KAFKA_USER:$KAFKA_GROUP" "$KAFKA_LOG_DIR" "$KAFKA_PID_DIR" "$KAFKA_DATA_DIR"
        chmod 755 "$KAFKA_LOG_DIR" "$KAFKA_PID_DIR" "$KAFKA_DATA_DIR"
    fi

    log_info "目录创建完成"
}

# 生成Kafka配置文件
generate_config() {
    local config_file="$KAFKA_CONF_DIR/server.properties"
    local backup_config="$KAFKA_CONF_DIR/server.properties.backup"

    # 备份原始配置
    if [ -f "$config_file" ] && [ ! -f "$backup_config" ]; then
        cp "$config_file" "$backup_config"
        log_info "备份原始配置文件"
    fi

    log_info "生成Kafka配置文件..."

    # 生成新的配置文件
    cat > "$config_file" <<EOF
# Kafka服务器配置文件
# 由启动脚本自动生成

# Broker ID
broker.id=$KAFKA_BROKER_ID

# 监听配置
listeners=$KAFKA_LISTENERS
advertised.listeners=$KAFKA_ADVERTISED_LISTENERS

# 日志目录
log.dirs=$KAFKA_DATA_DIR

# ZooKeeper连接
zookeeper.connect=$ZOOKEEPER_CONNECT

# 网络配置
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# 日志配置
num.partitions=3
default.replication.factor=1
min.insync.replicas=1
log.retention.hours=168
log.retention.check.interval.ms=300000
log.segment.bytes=1073741824

# 复制配置
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

# 内部主题配置
group.initial.rebalance.delay.ms=0

# 性能配置
num.replica.fetchers=2
num.recovery.threads.per.data.dir=2

# 安全配置
# security.inter.broker.protocol=PLAINTEXT
# sasl.mechanism.inter.broker.protocol=PLAIN
# sasl.enabled.mechanisms=PLAIN
EOF

    log_info "配置文件生成完成: $config_file"
}

# 启动ZooKeeper（如果配置了本地ZooKeeper）
start_zookeeper() {
    if [ -d "$ZOOKEEPER_HOME" ] && [ -f "$ZOOKEEPER_HOME/bin/zkServer.sh" ]; then
        log_info "启动本地ZooKeeper..."

        # 检查ZooKeeper是否已在运行
        if "$ZOOKEEPER_HOME/bin/zkServer.sh" status > /dev/null 2>&1; then
            log_info "ZooKeeper已在运行"
            return 0
        fi

        # 启动ZooKeeper
        sudo -u "$KAFKA_USER" "$ZOOKEEPER_HOME/bin/zkServer.sh" start
        if [ $? -eq 0 ]; then
            log_info "ZooKeeper启动成功"
        else
            log_error "ZooKeeper启动失败"
            exit 1
        fi

        # 等待ZooKeeper启动
        sleep 5
    fi
}

# 停止ZooKeeper（如果配置了本地ZooKeeper）
stop_zookeeper() {
    if [ -d "$ZOOKEEPER_HOME" ] && [ -f "$ZOOKEEPER_HOME/bin/zkServer.sh" ]; then
        log_info "停止本地ZooKeeper..."

        sudo -u "$KAFKA_USER" "$ZOOKEEPER_HOME/bin/zkServer.sh" stop
        log_info "ZooKeeper已停止"
    fi
}

# 启动Kafka Broker
start_kafka() {
    log_info "启动Kafka Broker (ID: $KAFKA_BROKER_ID)..."

    # 检查是否已在运行
    if [ -f "$KAFKA_PID_DIR/kafka.pid" ]; then
        local pid=$(cat "$KAFKA_PID_DIR/kafka.pid")
        if ps -p $pid > /dev/null 2>&1; then
            log_error "Kafka已在运行 (PID: $pid)"
            exit 1
        else
            rm -f "$KAFKA_PID_DIR/kafka.pid"
        fi
    fi

    # 生成配置文件
    generate_config

    # 设置JVM参数
    export KAFKA_HEAP_OPTS="$JAVA_OPTS"
    export KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true"
    export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_CONF_DIR/log4j.properties"

    # 启动Kafka
    local log_file="$KAFKA_LOG_DIR/kafka-server-$(date +%Y%m%d-%H%M%S).log"

    sudo -u "$KAFKA_USER" nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_CONF_DIR/server.properties" \
        > "$log_file" 2>&1 &

    local kafka_pid=$!
    echo $kafka_pid > "$KAFKA_PID_DIR/kafka.pid"

    log_info "Kafka已启动 (PID: $kafka_pid)"
    log_info "日志文件: $log_file"

    # 等待Kafka启动
    sleep 10

    # 检查Kafka状态
    if check_kafka_health; then
        log_info "Kafka启动成功！"
    else
        log_error "Kafka启动失败，请检查日志文件"
        exit 1
    fi
}

# 停止Kafka Broker
stop_kafka() {
    log_info "停止Kafka Broker..."

    if [ -f "$KAFKA_PID_DIR/kafka.pid" ]; then
        local pid=$(cat "$KAFKA_PID_DIR/kafka.pid")
        if ps -p $pid > /dev/null 2>&1; then
            log_info "发送终止信号给Kafka进程 $pid..."
            sudo -u "$KAFKA_USER" "$KAFKA_HOME/bin/kafka-server-stop.sh"

            # 等待进程停止
            local max_wait=30
            local wait_count=0
            while [ $wait_count -lt $max_wait ]; do
                if ! ps -p $pid > /dev/null 2>&1; then
                    log_info "Kafka已停止"
                    rm -f "$KAFKA_PID_DIR/kafka.pid"
                    return 0
                fi
                sleep 1
                wait_count=$((wait_count + 1))
            done

            # 强制终止
            log_warn "强制终止Kafka进程 $pid..."
            kill -9 $pid
            rm -f "$KAFKA_PID_DIR/kafka.pid"
        else
            log_warn "Kafka进程 $pid 不存在"
            rm -f "$KAFKA_PID_DIR/kafka.pid"
        fi
    else
        log_warn "Kafka PID文件不存在，尝试查找进程..."
        local pid=$(jps | grep kafka | awk '{print $1}')
        if [ -n "$pid" ]; then
            log_info "找到Kafka进程 $pid，正在终止..."
            kill -TERM $pid
            sleep 5
            if ps -p $pid > /dev/null 2>&1; then
                kill -9 $pid
            fi
        else
            log_warn "未找到运行中的Kafka进程"
        fi
    fi
}

# 检查Kafka健康状态
check_kafka_health() {
    log_info "检查Kafka健康状态..."

    # 检查Kafka端口
    if command -v nc > /dev/null 2>&1; then
        if nc -z "$KAFKA_HOST_NAME" "$KAFKA_PORT" > /dev/null 2>&1; then
            log_info "Kafka端口正常: $KAFKA_HOST_NAME:$KAFKA_PORT"
        else
            log_error "Kafka端口异常: $KAFKA_HOST_NAME:$KAFKA_PORT"
            return 1
        fi
    fi

    # 使用Kafka工具检查状态
    if [ -f "$KAFKA_HOME/bin/kafka-topics.sh" ]; then
        # 尝试获取topic列表
        local topics=$(timeout 10 "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_HOST_NAME:$KAFKA_PORT" --list 2>/dev/null | wc -l)
        if [ $? -eq 0 ]; then
            log_info "Kafka服务正常，当前有 $topics 个topic"
            return 0
        fi
    fi

    return 1
}

# 创建示例Topic
create_sample_topics() {
    log_info "创建示例Topic..."

    # 检查Kafka是否运行
    if ! check_kafka_health; then
        log_error "Kafka未运行，无法创建topic"
        return 1
    fi

    # 创建传感器数据topic
    local topics=("sensor-data" "robot-control" "system-log" "alert-events")

    for topic in "${topics[@]}"; do
        log_info "创建topic: $topic"
        "$KAFKA_HOME/bin/kafka-topics.sh" --create \
            --bootstrap-server "$KAFKA_HOST_NAME:$KAFKA_PORT" \
            --topic "$topic" \
            --partitions 3 \
            --replication-factor 1 \
            --if-not-exists \
            > /dev/null 2>&1

        if [ $? -eq 0 ]; then
            log_info "Topic '$topic' 创建成功"
        else
            log_warn "Topic '$topic' 已存在或创建失败"
        fi
    done
}

# 显示服务状态
show_status() {
    log_info "Kafka服务状态:"

    # 检查Kafka进程
    if jps | grep -q kafka; then
        log_info "  Kafka Broker: 运行中"
    else
        log_warn "  Kafka Broker: 未运行"
    fi

    # 检查ZooKeeper进程
    if [ -d "$ZOOKEEPER_HOME" ] && jps | grep -q QuorumPeerMain; then
        log_info "  ZooKeeper: 运行中"
    else
        log_warn "  ZooKeeper: 未运行"
    fi

    # 显示监听地址
    echo ""
    log_info "监听地址:"
    echo "  Kafka: $KAFKA_LISTENERS"
    echo "  ZooKeeper: $ZOOKEEPER_CONNECT"

    # 显示示例命令
    echo ""
    log_info "常用命令:"
    echo "  查看topic列表: $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server $KAFKA_HOST_NAME:$KAFKA_PORT"
    echo "  创建topic: $KAFKA_HOME/bin/kafka-topics.sh --create --topic \u003ctopic-name\u003e --bootstrap-server $KAFKA_HOST_NAME:$KAFKA_PORT"
    echo "  查看topic详情: $KAFKA_HOME/bin/kafka-topics.sh --describe --topic \u003ctopic-name\u003e --bootstrap-server $KAFKA_HOST_NAME:$KAFKA_PORT"
    echo "  消费消息: $KAFKA_HOME/bin/kafka-console-consumer.sh --topic \u003ctopic-name\u003e --from-beginning --bootstrap-server $KAFKA_HOST_NAME:$KAFKA_PORT"
}

# 清理日志和临时文件
cleanup() {
    log_info "清理Kafka日志和临时文件..."

    # 清理旧的日志文件（保留最近7天）
    if [ -d "$KAFKA_LOG_DIR" ]; then
        find "$KAFKA_LOG_DIR" -name "*.log" -type f -mtime +7 -delete 2>/dev/null || true
        find "$KAFKA_LOG_DIR" -name "*.log.*" -type f -mtime +7 -delete 2>/dev/null || true
        log_info "清理旧日志文件"
    fi

    # 清理临时文件
    if [ -d "/tmp/kafka-logs" ]; then
        find "/tmp/kafka-logs" -type f -mtime +1 -delete 2>/dev/null || true
        log_info "清理临时日志文件"
    fi

    log_info "清理完成"
}

# 显示帮助信息
show_help() {
    echo "Usage: $0 {start|stop|status|topics|cleanup|help} [--create-topics]"
    echo ""
    echo "命令:"
    echo "  start        - 启动Kafka服务"
    echo "  stop         - 停止Kafka服务"
    echo "  status       - 显示服务状态"
    echo "  topics       - 创建示例topics"
    echo "  cleanup      - 清理日志和临时文件"
    echo "  help         - 显示帮助信息"
    echo ""
    echo "选项:"
    echo "  --create-topics - 在启动后创建示例topics"
    echo ""
    echo "环境变量:"
    echo "  KAFKA_HOME                 - Kafka安装目录 (默认: /opt/kafka)"
    echo "  KAFKA_CONF_DIR             - Kafka配置目录 (默认: \$KAFKA_HOME/config)"
    echo "  KAFKA_LOG_DIR              - 日志目录 (默认: \$KAFKA_HOME/logs)"
    echo "  KAFKA_PID_DIR              - PID文件目录 (默认: \$KAFKA_HOME/run)"
    echo "  KAFKA_DATA_DIR             - 数据目录 (默认: \$KAFKA_HOME/data)"
    echo "  KAFKA_BROKER_ID            - Broker ID (默认: 0)"
    echo "  KAFKA_PORT                 - Kafka端口 (默认: 9092)"
    echo "  KAFKA_HOST_NAME            - Kafka主机名 (默认: 自动检测)"
    echo "  KAFKA_LISTENERS            - 监听器配置 (默认: PLAINTEXT://0.0.0.0:9092)"
    echo "  KAFKA_ADVERTISED_LISTENERS - 广播监听器配置 (默认: PLAINTEXT://\$KAFKA_HOST_NAME:9092)"
    echo "  ZOOKEEPER_CONNECT          - ZooKeeper连接地址 (默认: localhost:2181)"
    echo "  ZOOKEEPER_HOME             - ZooKeeper安装目录 (默认: /opt/zookeeper)"
    echo "  JAVA_HOME                  - Java安装目录"
    echo "  JAVA_OPTS                  - JVM参数"
    echo "  KAFKA_USER                 - 运行Kafka的用户 (默认: kafka)"
    echo "  KAFKA_GROUP                - 运行Kafka的用户组 (默认: kafka)"
}

# 主函数
main() {
    local command=$1
    shift || true

    case "$command" in
        start)
            check_environment
            create_directories
            start_zookeeper
            start_kafka
            if [ "$1" = "--create-topics" ]; then
                create_sample_topics
            fi
            show_status
            ;;
        stop)
            stop_kafka
            stop_zookeeper
            ;;
        status)
            show_status
            ;;
        topics)
            create_sample_topics
            ;;
        cleanup)
            cleanup
            ;;
        help|--help|-h|"")
            show_help
            ;;
        *)
            log_error "未知命令: $command"
            show_help
            exit 1
            ;;
    esac
}

# 脚本入口
main "$@"

exit 0