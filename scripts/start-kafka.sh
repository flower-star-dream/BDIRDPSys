#!/bin/bash

# BDIRDPSys Kafka启动脚本
# 用于启动Kafka集群服务

set -e

# 配置变量
KAFKA_HOME=${KAFKA_HOME:-/opt/kafka}
KAFKA_CONF_DIR=${KAFKA_CONF_DIR:-$KAFKA_HOME/config}
KAFKA_LOG_DIR=${KAFKA_LOG_DIR:-/var/log/kafka}
KAFKA_PID_DIR=${KAFKA_PID_DIR:-/var/run/kafka}
ZOOKEEPER_CONNECT=${ZOOKEEPER_CONNECT:-localhost:2181}

# Kafka集群节点（如果有多个Broker）
KAFKA_BROKERS=${KAFKA_BROKERS:-localhost:9092}

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

# 检查环境
check_environment() {
    log_info "检查Kafka环境..."

    if [ ! -d "$KAFKA_HOME" ]; then
        log_error "KAFKA_HOME目录不存在: $KAFKA_HOME"
        exit 1
    fi

    if [ ! -f "$KAFKA_HOME/bin/kafka-server-start.sh" ]; then
        log_error "Kafka启动脚本不存在: $KAFKA_HOME/bin/kafka-server-start.sh"
        exit 1
    fi

    # 检查Java环境
    if ! command -v java > /dev/null 2>&1; then
        log_error "Java未安装或未配置环境变量"
        exit 1
    fi

    # 检查Zookeeper连接
    if ! nc -z $(echo $ZOOKEEPER_CONNECT | cut -d':' -f1) $(echo $ZOOKEEPER_CONNECT | cut -d':' -f2) 2>/dev/null; then
        log_warn "Zookeeper服务可能未启动: $ZOOKEEPER_CONNECT"
        log_warn "请确保Zookeeper服务已启动"
    fi

    log_info "环境检查通过"
}

# 创建必要的目录
create_directories() {
    log_info "创建必要的目录..."

    # 创建日志目录
    mkdir -p "$KAFKA_LOG_DIR"
    mkdir -p "$KAFKA_PID_DIR"

    # 创建数据目录
    mkdir -p /tmp/kafka-logs
    mkdir -p /tmp/zookeeper

    # 设置权限
    chmod 755 "$KAFKA_LOG_DIR"
    chmod 755 "$KAFKA_PID_DIR"

    log_info "目录创建完成"
}

# 检查Zookeeper状态
check_zookeeper() {
    log_info "检查Zookeeper状态..."

    # 等待Zookeeper启动
    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if echo "ruok" | nc $(echo $ZOOKEEPER_CONNECT | cut -d':' -f1) $(echo $ZOOKEEPER_CONNECT | cut -d':' -f2) 2>/dev/null | grep -q "imok"; then
            log_info "Zookeeper服务正常"
            return 0
        fi
        attempt=$((attempt + 1))
        log_info "等待Zookeeper启动... ($attempt/$max_attempts)"
        sleep 2
    done

    log_error "Zookeeper服务未响应"
    return 1
}

# 启动Kafka Broker
start_kafka_broker() {
    local broker_id=$1
    local port=$2
    local log_dir=$3

    log_info "启动Kafka Broker (ID: $broker_id, Port: $port)..."

    # 创建Broker配置文件
    local broker_config="$KAFKA_CONF_DIR/server-$broker_id.properties"

    cat > "$broker_config" << EOF
# Kafka Broker配置
broker.id=$broker_id
port=$port
log.dirs=$log_dir
zookeeper.connect=$ZOOKEEPER_CONNECT
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=3
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0

# 性能优化配置
compression.type=snappy
num.replica.fetchers=4
replica.fetch.max.bytes=1048576
replica.socket.receive.buffer.bytes=65536

# JMX配置
JMX_PORT=$((9999 + broker_id))
EOF

    # 启动Kafka Broker
    nohup $KAFKA_HOME/bin/kafka-server-start.sh "$broker_config" \
        > "$KAFKA_LOG_DIR/kafka-$broker_id.log" 2>&1 &

    local kafka_pid=$!
    echo $kafka_pid > "$KAFKA_PID_DIR/kafka-$broker_id.pid"

    # 等待Broker启动
    sleep 10

    # 检查Broker是否启动成功
    if ps -p $kafka_pid > /dev/null 2>&1; then
        log_info "Kafka Broker $broker_id 启动成功 (PID: $kafka_pid, Port: $port)"
        return 0
    else
        log_error "Kafka Broker $broker_id 启动失败"
        return 1
    fi
}

# 启动单个Kafka Broker
start_single_broker() {
    log_info "启动单个Kafka Broker..."

    # 创建数据目录
    local log_dir="/tmp/kafka-logs-1"
    mkdir -p "$log_dir"

    start_kafka_broker 1 9092 "$log_dir"
}

# 启动Kafka集群
start_kafka_cluster() {
    log_info "启动Kafka集群..."

    local brokers=($(echo $KAFKA_BROKERS | tr ',' ' '))
    local broker_id=1

    for broker in "${brokers[@]}"; do
        local host=$(echo $broker | cut -d':' -f1)
        local port=$(echo $broker | cut -d':' -f2)
        local log_dir="/tmp/kafka-logs-$broker_id"

        mkdir -p "$log_dir"

        if [ "$host" = "localhost" ] || [ "$host" = "127.0.0.1" ]; then
            start_kafka_broker $broker_id $port "$log_dir"
        else
            log_info "远程Broker $broker 需要在对应主机上启动"
        fi

        broker_id=$((broker_id + 1))
    done
}

# 创建必要的Topic
create_topics() {
    log_info "创建必要的Topic..."

    # 等待Kafka启动
    sleep 15

    # 传感器数据Topic
    local topics=(
        "sensor-data-temperature:6:2"
        "sensor-data-humidity:6:2"
        "sensor-data-pressure:6:2"
        "sensor-data-position:6:2"
        "sensor-data-general:12:2"
        "robot-commands:3:2"
        "robot-status:3:2"
        "robot-alerts:3:2"
        "system-events:3:2"
    )

    for topic_config in "${topics[@]}"; do
        local topic=$(echo $topic_config | cut -d':' -f1)
        local partitions=$(echo $topic_config | cut -d':' -f2)
        local replication=$(echo $topic_config | cut -d':' -f3)

        log_info "创建Topic: $topic (partitions: $partitions, replication: $replication)"

        $KAFKA_HOME/bin/kafka-topics.sh --create \
            --bootstrap-server ${KAFKA_BROKERS%%,*} \
            --topic "$topic" \
            --partitions $partitions \
            --replication-factor $replication \
            --if-not-exists
    done

    log_info "Topic创建完成"
}

# 检查服务状态
check_services() {
    log_info "检查Kafka服务状态..."

    local brokers=($(echo $KAFKA_BROKERS | tr ',' ' '))
    local running_brokers=()

    for broker in "${brokers[@]}"; do
        local host=$(echo $broker | cut -d':' -f1)
        local port=$(echo $broker | cut -d':' -f2)

        if nc -z $host $port 2>/dev/null; then
            running_brokers+=("$broker")
            log_info "Kafka Broker $broker 正在运行"
        else
            log_warn "Kafka Broker $broker 未运行"
        fi
    done

    log_info "当前运行的Broker: ${running_brokers[*]}"

    # 检查Topic列表
    log_info "Topic列表:"
    $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server ${KAFKA_BROKERS%%,*}
}

# 显示Kafka工具命令
show_kafka_tools() {
    log_info "Kafka常用命令:"
    echo "  查看Topic列表: kafka-topics.sh --list --bootstrap-server ${KAFKA_BROKERS%%,*}"
    echo "  查看Topic详情: kafka-topics.sh --describe --topic <topic-name> --bootstrap-server ${KAFKA_BROKERS%%,*}"
    echo "  生产消息: kafka-console-producer.sh --topic <topic-name> --bootstrap-server ${KAFKA_BROKERS%%,*}"
    echo "  消费消息: kafka-console-consumer.sh --topic <topic-name> --from-beginning --bootstrap-server ${KAFKA_BROKERS%%,*}"
    echo "  查看消费者组: kafka-consumer-groups.sh --list --bootstrap-server ${KAFKA_BROKERS%%,*}"
    echo "  查看消费者组详情: kafka-consumer-groups.sh --describe --group <group-name> --bootstrap-server ${KAFKA_BROKERS%%,*}"
}

# 启动Kafka Manager（可选）
start_kafka_manager() {
    if [ -f "$KAFKA_HOME/bin/kafka-manager" ]; then
        log_info "启动Kafka Manager..."
        nohup $KAFKA_HOME/bin/kafka-manager \
            -Dconfig.file=$KAFKA_HOME/config/kafka-manager.conf \
            > "$KAFKA_LOG_DIR/kafka-manager.log" 2>&1 &

        local manager_pid=$!
        echo $manager_pid > "$KAFKA_PID_DIR/kafka-manager.pid"

        log_info "Kafka Manager已启动 (PID: $manager_pid)"
        log_info "Kafka Manager UI: http://localhost:9000/"
    fi
}

# 主函数
main() {
    log_info "开始启动Kafka集群..."

    check_environment
    create_directories

    # 检查Zookeeper
    if ! check_zookeeper; then
        log_error "Zookeeper检查失败，请确保Zookeeper服务已启动"
        exit 1
    fi

    # 启动Kafka
    if [ "$(echo $KAFKA_BROKERS | tr ',' ' ' | wc -w)" -gt 1 ]; then
        start_kafka_cluster
    else
        start_single_broker
    fi

    # 创建Topic
    create_topics

    # 检查状态
    check_services

    # 启动Kafka Manager（如果存在）
    start_kafka_manager

    log_info "Kafka集群启动完成！"
    show_kafka_tools
}

# 停止Kafka
stop_kafka() {
    log_info "停止Kafka服务..."

    # 停止Kafka Manager
    if [ -f "$KAFKA_PID_DIR/kafka-manager.pid" ]; then
        local manager_pid=$(cat "$KAFKA_PID_DIR/kafka-manager.pid")
        if ps -p $manager_pid > /dev/null 2>&1; then
            kill -TERM $manager_pid
            log_info "Kafka Manager已停止 (PID: $manager_pid)"
        fi
        rm -f "$KAFKA_PID_DIR/kafka-manager.pid"
    fi

    # 停止所有Kafka Broker
    for pid_file in "$KAFKA_PID_DIR"/kafka-*.pid; do
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            if ps -p $pid > /dev/null 2>&1; then
                kill -TERM $pid
                local broker_id=$(basename "$pid_file" .pid | sed 's/kafka-//')
                log_info "Kafka Broker $broker_id 已停止 (PID: $pid)"
            fi
            rm -f "$pid_file"
        fi
    done

    # 等待进程完全停止
    sleep 5

    # 强制停止残留的进程
    local pids=$(jps | grep Kafka | awk '{print $1}')
    if [ -n "$pids" ]; then
        log_warn "强制停止残留的Kafka进程: $pids"
        kill -9 $pids 2>/dev/null || true
    fi

    log_info "Kafka服务已停止"
}

# 脚本入口
case "$1" in
    start)
        main
        ;;
    stop)
        stop_kafka
        ;;
    status)
        check_services
        ;;
    restart)
        $0 stop
        sleep 5
        $0 start
        ;;
    create-topic)
        if [ -z "$2" ]; then
            echo "Usage: $0 create-topic <topic-name> [partitions] [replication]"
            exit 1
        fi
        local topic=$2
        local partitions=${3:-6}
        local replication=${4:-1}
        $KAFKA_HOME/bin/kafka-topics.sh --create \
            --bootstrap-server ${KAFKA_BROKERS%%,*} \
            --topic "$topic" \
            --partitions $partitions \
            --replication-factor $replication
        ;;
    delete-topic)
        if [ -z "$2" ]; then
            echo "Usage: $0 delete-topic <topic-name>"
            exit 1
        fi
        local topic=$2
        $KAFKA_HOME/bin/kafka-topics.sh --delete \
            --bootstrap-server ${KAFKA_BROKERS%%,*} \
            --topic "$topic"
        ;;
    list-topics)
        $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server ${KAFKA_BROKERS%%,*}
        ;;
    *)
        echo "Usage: $0 {start|stop|status|restart|create-topic|delete-topic|list-topics}"
        echo "  start          - 启动Kafka服务"
        echo "  stop           - 停止Kafka服务"
        echo "  status         - 检查服务状态"
        echo "  restart        - 重启Kafka服务"
        echo "  create-topic   - 创建Topic (参数: topic名 [分区数] [副本数])"
        echo "  delete-topic   - 删除Topic (参数: topic名)"
        echo "  list-topics    - 列出所有Topic"
        exit 1
        ;;
esac

exit 0

# Systemd服务文件（可选）
# 保存为 /etc/systemd/system/kafka.service
: '
[Unit]
Description=Apache Kafka
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=forking
User=kafka
Group=kafka
Environment=KAFKA_HOME=/opt/kafka
Environment=KAFKA_CONF_DIR=/opt/kafka/config
ExecStart=/opt/kafka/bin/start-kafka.sh start
ExecStop=/opt/kafka/bin/start-kafka.sh stop
ExecReload=/opt/kafka/bin/start-kafka.sh restart
PIDFile=/var/run/kafka/kafka.pid
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
'