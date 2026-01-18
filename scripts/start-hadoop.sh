#!/bin/bash

# BDIRDPSys Hadoop启动脚本
# 用于启动Hadoop集群服务

set -e

# 配置变量
HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}
HADOOP_LOG_DIR=${HADOOP_LOG_DIR:-/var/log/hadoop}
HADOOP_PID_DIR=${HADOOP_PID_DIR:-/var/run/hadoop}

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
    log_info "检查Hadoop环境..."

    if [ ! -d "$HADOOP_HOME" ]; then
        log_error "HADOOP_HOME目录不存在: $HADOOP_HOME"
        exit 1
    fi

    if [ ! -f "$HADOOP_HOME/bin/hadoop" ]; then
        log_error "Hadoop可执行文件不存在: $HADOOP_HOME/bin/hadoop"
        exit 1
    fi

    # 检查Java环境
    if ! command -v java > /dev/null 2>&1; then
        log_error "Java未安装或未配置环境变量"
        exit 1
    fi

    # 检查HADOOP_CONF_DIR
    if [ ! -d "$HADOOP_CONF_DIR" ]; then
        log_error "Hadoop配置目录不存在: $HADOOP_CONF_DIR"
        exit 1
    fi

    log_info "环境检查通过"
}

# 创建必要的目录
create_directories() {
    log_info "创建必要的目录..."

    # 创建日志目录
    mkdir -p "$HADOOP_LOG_DIR"
    mkdir -p "$HADOOP_PID_DIR"
    mkdir -p /tmp/hadoop-${USER}

    # 设置权限
    chmod 755 "$HADOOP_LOG_DIR"
    chmod 755 "$HADOOP_PID_DIR"

    log_info "目录创建完成"
}

# 启动HDFS
start_hdfs() {
    log_info "启动HDFS服务..."

    # 格式化NameNode（首次启动）
    if [ ! -d "$HADOOP_HOME/data/dfs/name/current" ]; then
        log_info "格式化NameNode..."
        $HADOOP_HOME/bin/hdfs namenode -format -force
    fi

    # 启动NameNode
    log_info "启动NameNode..."
    $HADOOP_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode

    # 等待NameNode启动
    sleep 5

    # 检查NameNode是否启动成功
    if ! jps | grep -q NameNode; then
        log_error "NameNode启动失败"
        return 1
    fi

    # 启动DataNode
    log_info "启动DataNode..."
    $HADOOP_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start datanode

    # 等待DataNode启动
    sleep 3

    # 检查DataNode是否启动成功
    if ! jps | grep -q DataNode; then
        log_warn "DataNode可能未启动成功"
    fi

    log_info "HDFS服务启动完成"
}

# 启动YARN
start_yarn() {
    log_info "启动YARN服务..."

    # 启动ResourceManager
    log_info "启动ResourceManager..."
    $HADOOP_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start resourcemanager

    # 等待ResourceManager启动
    sleep 5

    # 检查ResourceManager是否启动成功
    if ! jps | grep -q ResourceManager; then
        log_error "ResourceManager启动失败"
        return 1
    fi

    # 启动NodeManager
    log_info "启动NodeManager..."
    $HADOOP_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start nodemanager

    # 等待NodeManager启动
    sleep 3

    # 检查NodeManager是否启动成功
    if ! jps | grep -q NodeManager; then
        log_warn "NodeManager可能未启动成功"
    fi

    log_info "YARN服务启动完成"
}

# 启动历史服务器
start_historyserver() {
    log_info "启动JobHistory Server..."

    $HADOOP_HOME/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR start historyserver

    # 等待历史服务器启动
    sleep 3

    if jps | grep -q JobHistoryServer; then
        log_info "JobHistory Server启动成功"
    else
        log_warn "JobHistory Server可能未启动成功"
    fi
}

# 检查服务状态
check_services() {
    log_info "检查服务状态..."

    local services=("NameNode" "DataNode" "ResourceManager" "NodeManager" "JobHistoryServer")
    local running_services=()

    for service in "${services[@]}"; do
        if jps | grep -q "$service"; then
            running_services+=("$service")
            log_info "$service 正在运行"
        else
            log_warn "$service 未运行"
        fi
    done

    log_info "当前运行的服务: ${running_services[*]}"
}

# 显示Web UI地址
show_web_ui() {
    log_info "Hadoop Web UI地址:"
    echo "  NameNode: http://localhost:9870/"
    echo "  ResourceManager: http://localhost:8088/"
    echo "  NodeManager: http://localhost:8042/"
    echo "  JobHistory: http://localhost:19888/"
}

# 主函数
main() {
    log_info "开始启动Hadoop集群..."

    # 检查是否以root用户运行
    if [ "$EUID" -eq 0 ]; then
        log_warn "不建议以root用户运行Hadoop"
    fi

    check_environment
    create_directories

    # 启动服务
    start_hdfs
    start_yarn
    start_historyserver

    # 检查状态
    check_services
    show_web_ui

    log_info "Hadoop集群启动完成！"

    # 创建HDFS目录
    log_info "创建必要的HDFS目录..."
    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/${USER}
    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /data/sensor
    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /data/robot
    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /tmp/hive
    $HADOOP_HOME/bin/hdfs dfs -chmod -R 777 /tmp
    $HADOOP_HOME/bin/hdfs dfs -chmod -R 755 /user
    $HADOOP_HOME/bin/hdfs dfs -chmod -R 755 /data

    log_info "HDFS目录创建完成"
}

# 脚本入口
case "$1" in
    start)
        main
        ;;
    stop)
        log_info "停止Hadoop服务..."
        $HADOOP_HOME/sbin/stop-dfs.sh
        $HADOOP_HOME/sbin/stop-yarn.sh
        $HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver
        log_info "Hadoop服务已停止"
        ;;
    status)
        check_services
        ;;
    restart)
        $0 stop
        sleep 5
        $0 start
        ;;
    *)
        echo "Usage: $0 {start|stop|status|restart}"
        exit 1
        ;;
esac

exit 0

# Systemd服务文件（可选）
# 保存为 /etc/systemd/system/hadoop.service
: '
[Unit]
Description=Hadoop Cluster
After=network.target

[Service]
Type=forking
User=hadoop
Group=hadoop
Environment=HADOOP_HOME=/opt/hadoop
Environment=HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ExecStart=/opt/hadoop/sbin/start-hadoop.sh start
ExecStop=/opt/hadoop/sbin/start-hadoop.sh stop
ExecReload=/opt/hadoop/sbin/start-hadoop.sh restart
PIDFile=/var/run/hadoop/hadoop.pid
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
'