#!/bin/bash

# BDIRDPSys Hadoop启动脚本
# 用于启动Hadoop集群服务

set -e

# 配置变量 - 使用环境变量或默认值
HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
HADOOP_LOG_DIR="${HADOOP_LOG_DIR:-$HADOOP_HOME/logs}"
HADOOP_PID_DIR="${HADOOP_PID_DIR:-$HADOOP_HOME/run}"
HADOOP_DATA_DIR="${HADOOP_DATA_DIR:-$HADOOP_HOME/data}"
HADOOP_TEMP_DIR="${HADOOP_TEMP_DIR:-$HADOOP_HOME/tmp}"

# Java配置
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk}"

# 用户配置
HADOOP_USER="${HADOOP_USER:-hadoop}"
HADOOP_GROUP="${HADOOP_GROUP:-hadoop}"

# 端口配置
NAMENODE_PORT="${NAMENODE_PORT:-9870}"
DATANODE_PORT="${DATANODE_PORT:-9864}"
RESOURCEMANAGER_PORT="${RESOURCEMANAGER_PORT:-8088}"
NODEMANAGER_PORT="${NODEMANAGER_PORT:-8042}"

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
    log_info "检查Hadoop环境..."

    # 检查Hadoop安装目录
    if [ ! -d "$HADOOP_HOME" ]; then
        log_error "HADOOP_HOME目录不存在: $HADOOP_HOME"
        log_info "请设置HADOOP_HOME环境变量或安装Hadoop"
        exit 1
    fi

    # 检查Hadoop可执行文件
    if [ ! -f "$HADOOP_HOME/bin/hadoop" ]; then
        log_error "Hadoop可执行文件不存在: $HADOOP_HOME/bin/hadoop"
        exit 1
    fi

    # 检查配置文件
    if [ ! -f "$HADOOP_CONF_DIR/core-site.xml" ]; then
        log_error "Hadoop配置文件不存在: $HADOOP_CONF_DIR/core-site.xml"
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
    if [ "$(id -u)" -ne 0 ] && [ "$(whoami)" != "$HADOOP_USER" ]; then
        log_warn "建议使用 $HADOOP_USER 用户运行此脚本"
    fi

    # 检查环境变量
    if [ -z "$HADOOP_CONF_DIR" ]; then
        export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
        log_info "设置 HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
    fi

    # 检查端口是否被占用
    check_ports

    log_info "环境检查通过"
}

# 检查端口是否被占用
check_ports() {
    log_info "检查端口状态..."

    # 检查NameNode端口
    if command -v lsof > /dev/null 2>&1; then
        if lsof -i :$NAMENODE_PORT > /dev/null 2>&1; then
            log_warn "NameNode端口 $NAMENODE_PORT 已被占用"
        fi
        if lsof -i :$DATANODE_PORT > /dev/null 2>&1; then
            log_warn "DataNode端口 $DATANODE_PORT 已被占用"
        fi
        if lsof -i :$RESOURCEMANAGER_PORT > /dev/null 2>&1; then
            log_warn "ResourceManager端口 $RESOURCEMANAGER_PORT 已被占用"
        fi
        if lsof -i :$NODEMANAGER_PORT > /dev/null 2>&1; then
            log_warn "NodeManager端口 $NODEMANAGER_PORT 已被占用"
        fi
    fi
}

# 创建必要的目录
create_directories() {
    log_info "创建Hadoop必要的目录..."

    # 创建日志目录
    mkdir -p "$HADOOP_LOG_DIR"
    mkdir -p "$HADOOP_PID_DIR"
    mkdir -p "$HADOOP_DATA_DIR"
    mkdir -p "$HADOOP_TEMP_DIR"
    mkdir -p "$HADOOP_HOME/logs/userlogs"

    # 创建NameNode和DataNode数据目录
    local namenode_dir=$(grep -A 1 "dfs.namenode.name.dir" "$HADOOP_CONF_DIR/hdfs-site.xml" 2>/dev/null | grep "value" | sed 's/.*\u003cvalue\u003e\(.*\)\u003c\/value\u003e.*/\1/' || echo "$HADOOP_DATA_DIR/namenode")
    local datanode_dir=$(grep -A 1 "dfs.datanode.data.dir" "$HADOOP_CONF_DIR/hdfs-site.xml" 2>/dev/null | grep "value" | sed 's/.*\u003cvalue\u003e\(.*\)\u003c\/value\u003e.*/\1/' || echo "$HADOOP_DATA_DIR/datanode")

    mkdir -p "$namenode_dir"
    mkdir -p "$datanode_dir"

    # 设置权限
    if [ "$(whoami)" = "root" ]; then
        chown -R "$HADOOP_USER:$HADOOP_GROUP" "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR" "$HADOOP_DATA_DIR" "$HADOOP_TEMP_DIR"
        chmod 755 "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR" "$HADOOP_DATA_DIR" "$HADOOP_TEMP_DIR"
    fi

    log_info "目录创建完成"
}

# 格式化NameNode
format_namenode() {
    if [ "$1" = "--format" ]; then
        log_info "格式化NameNode..."

        # 检查是否已格式化
        local namenode_dir=$(grep -A 1 "dfs.namenode.name.dir" "$HADOOP_CONF_DIR/hdfs-site.xml" 2>/dev/null | grep "value" | sed 's/.*\u003cvalue\u003e\(.*\)\u003c\/value\u003e.*/\1/' || echo "$HADOOP_DATA_DIR/namenode")

        if [ -d "$namenode_dir/current" ]; then
            log_warn "NameNode已格式化，跳过格式化步骤"
            return 0
        fi

        # 格式化NameNode
        sudo -u "$HADOOP_USER" "$HADOOP_HOME/bin/hdfs" namenode -format -force
        if [ $? -ne 0 ]; then
            log_error "NameNode格式化失败"
            exit 1
        fi

        log_info "NameNode格式化完成"
    fi
}

# 启动HDFS
start_hdfs() {
    log_info "启动HDFS服务..."

    # 启动NameNode
    log_info "启动NameNode..."
    sudo -u "$HADOOP_USER" "$HADOOP_HOME/sbin/hadoop-daemon.sh" --config "$HADOOP_CONF_DIR" start namenode
    if [ $? -ne 0 ]; then
        log_error "NameNode启动失败"
        exit 1
    fi

    # 等待NameNode启动
    sleep 5

    # 启动DataNode
    log_info "启动DataNode..."
    sudo -u "$HADOOP_USER" "$HADOOP_HOME/sbin/hadoop-daemon.sh" --config "$HADOOP_CONF_DIR" start datanode
    if [ $? -ne 0 ]; then
        log_error "DataNode启动失败"
        exit 1
    fi

    # 等待DataNode启动
    sleep 3

    # 检查HDFS状态
    if "$HADOOP_HOME/bin/hdfs" dfsadmin -report > /dev/null 2>&1; then
        log_info "HDFS服务启动成功"
    else
        log_error "HDFS服务状态异常"
        exit 1
    fi
}

# 停止HDFS
stop_hdfs() {
    log_info "停止HDFS服务..."

    # 停止DataNode
    sudo -u "$HADOOP_USER" "$HADOOP_HOME/sbin/hadoop-daemon.sh" stop datanode

    # 停止NameNode
    sudo -u "$HADOOP_USER" "$HADOOP_HOME/sbin/hadoop-daemon.sh" stop namenode

    log_info "HDFS服务已停止"
}

# 启动YARN
start_yarn() {
    log_info "启动YARN服务..."

    # 启动ResourceManager
    log_info "启动ResourceManager..."
    sudo -u "$HADOOP_USER" "$HADOOP_HOME/sbin/yarn-daemon.sh" --config "$HADOOP_CONF_DIR" start resourcemanager
    if [ $? -ne 0 ]; then
        log_error "ResourceManager启动失败"
        exit 1
    fi

    # 等待ResourceManager启动
    sleep 5

    # 启动NodeManager
    log_info "启动NodeManager..."
    sudo -u "$HADOOP_USER" "$HADOOP_HOME/sbin/yarn-daemon.sh" --config "$HADOOP_CONF_DIR" start nodemanager
    if [ $? -ne 0 ]; then
        log_error "NodeManager启动失败"
        exit 1
    fi

    # 等待NodeManager启动
    sleep 3

    # 检查YARN状态
    if curl -s "http://localhost:$RESOURCEMANAGER_PORT/ws/v1/cluster/info" > /dev/null 2>&1; then
        log_info "YARN服务启动成功"
    else
        log_warn "YARN服务状态异常，请检查日志"
    fi
}

# 停止YARN
stop_yarn() {
    log_info "停止YARN服务..."

    # 停止NodeManager
    sudo -u "$HADOOP_USER" "$HADOOP_HOME/sbin/yarn-daemon.sh" stop nodemanager

    # 停止ResourceManager
    sudo -u "$HADOOP_USER" "$HADOOP_HOME/sbin/yarn-daemon.sh" stop resourcemanager

    log_info "YARN服务已停止"
}

# 启动历史服务器
start_historyserver() {
    log_info "启动JobHistory Server..."

    sudo -u "$HADOOP_USER" "$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh" --config "$HADOOP_CONF_DIR" start historyserver
    if [ $? -ne 0 ]; then
        log_error "JobHistory Server启动失败"
        exit 1
    fi

    log_info "JobHistory Server启动完成"
}

# 停止历史服务器
stop_historyserver() {
    log_info "停止JobHistory Server..."

    sudo -u "$HADOOP_USER" "$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh" stop historyserver

    log_info "JobHistory Server已停止"
}

# 显示服务状态
show_status() {
    log_info "Hadoop服务状态:"

    # 检查NameNode
    if jps | grep -q NameNode; then
        log_info "  NameNode: 运行中"
    else
        log_warn "  NameNode: 未运行"
    fi

    # 检查DataNode
    if jps | grep -q DataNode; then
        log_info "  DataNode: 运行中"
    else
        log_warn "  DataNode: 未运行"
    fi

    # 检查ResourceManager
    if jps | grep -q ResourceManager; then
        log_info "  ResourceManager: 运行中"
    else
        log_warn "  ResourceManager: 未运行"
    fi

    # 检查NodeManager
    if jps | grep -q NodeManager; then
        log_info "  NodeManager: 运行中"
    else
        log_warn "  NodeManager: 未运行"
    fi

    # 检查JobHistory Server
    if jps | grep -q JobHistoryServer; then
        log_info "  JobHistory Server: 运行中"
    else
        log_warn "  JobHistory Server: 未运行"
    fi

    # 显示Web UI地址
    echo ""
    log_info "Web UI地址:"
    echo "  NameNode: http://localhost:$NAMENODE_PORT/"
    echo "  ResourceManager: http://localhost:$RESOURCEMANAGER_PORT/"
    echo "  NodeManager: http://localhost:$NODEMANAGER_PORT/"
}

# 检查HDFS健康状态
check_hdfs_health() {
    log_info "检查HDFS健康状态..."

    # 检查HDFS是否健康
    local hdfs_report=$("$HADOOP_HOME/bin/hdfs" dfsadmin -report 2>/dev/null | grep "Live datanodes" || echo "0")
    local live_nodes=$(echo "$hdfs_report" | grep -o "[0-9]\+" || echo "0")

    if [ "$live_nodes" -gt 0 ]; then
        log_info "HDFS健康: $live_nodes 个DataNode在线"
        return 0
    else
        log_error "HDFS异常: 没有DataNode在线"
        return 1
    fi
}

# 清理临时文件和日志
cleanup() {
    log_info "清理Hadoop临时文件和日志..."

    # 清理临时目录
    if [ -d "$HADOOP_TEMP_DIR" ]; then
        rm -rf "$HADOOP_TEMP_DIR"/*
        log_info "清理临时目录: $HADOOP_TEMP_DIR"
    fi

    # 清理旧的日志文件（保留最近7天）
    if [ -d "$HADOOP_LOG_DIR" ]; then
        find "$HADOOP_LOG_DIR" -name "*.log" -type f -mtime +7 -delete 2>/dev/null || true
        find "$HADOOP_LOG_DIR" -name "*.out" -type f -mtime +7 -delete 2>/dev/null || true
        log_info "清理旧日志文件"
    fi

    # 清理用户日志
    if [ -d "$HADOOP_HOME/logs/userlogs" ]; then
        find "$HADOOP_HOME/logs/userlogs" -type f -mtime +3 -delete 2>/dev/null || true
        log_info "清理用户日志"
    fi

    log_info "清理完成"
}

# 安全模式操作
safemode() {
    local operation=$1
    case "$operation" in
        enter)
            log_info "进入安全模式..."
            "$HADOOP_HOME/bin/hdfs" dfsadmin -safemode enter
            ;;
        leave)
            log_info "离开安全模式..."
            "$HADOOP_HOME/bin/hdfs" dfsadmin -safemode leave
            ;;
        get)
            local status=$("$HADOOP_HOME/bin/hdfs" dfsadmin -safemode get 2>/dev/null | grep -o "ON\|OFF" || echo "UNKNOWN")
            log_info "安全模式状态: $status"
            ;;
        *)
            log_error "未知的安全模式操作: $operation"
            echo "用法: $0 safemode {enter|leave|get}"
            ;;
    esac
}

# 均衡HDFS
balancer() {
    log_info "运行HDFS均衡器..."

    # 检查是否需要在安全模式下运行
    local safemode_status=$("$HADOOP_HOME/bin/hdfs" dfsadmin -safemode get 2>/dev/null | grep -o "ON\|OFF" || echo "OFF")
    if [ "$safemode_status" = "ON" ]; then
        log_error "HDFS处于安全模式，无法运行均衡器"
        exit 1
    fi

    # 运行均衡器
    sudo -u "$HADOOP_USER" "$HADOOP_HOME/sbin/start-balancer.sh" -threshold 10
    if [ $? -eq 0 ]; then
        log_info "HDFS均衡器运行完成"
    else
        log_error "HDFS均衡器运行失败"
    fi
}

# 显示帮助信息
show_help() {
    echo "Usage: $0 {start|stop|status|health|format|cleanup|safemode|balancer|help} [--format]"
    echo ""
    echo "命令:"
    echo "  start    - 启动所有Hadoop服务"
    echo "  stop     - 停止所有Hadoop服务"
    echo "  status   - 显示服务状态"
    echo "  health   - 检查HDFS健康状态"
    echo "  format   - 格式化NameNode（谨慎使用）"
    echo "  cleanup  - 清理临时文件和日志"
    echo "  safemode - 安全模式操作"
    echo "  balancer - 运行HDFS均衡器"
    echo "  help     - 显示帮助信息"
    echo ""
    echo "选项:"
    echo "  --format - 在启动前格式化NameNode"
    echo ""
    echo "安全模式:"
    echo "  $0 safemode enter  - 进入安全模式"
    echo "  $0 safemode leave  - 离开安全模式"
    echo "  $0 safemode get    - 获取安全模式状态"
    echo ""
    echo "环境变量:"
    echo "  HADOOP_HOME                - Hadoop安装目录 (默认: /opt/hadoop)"
    echo "  HADOOP_CONF_DIR            - Hadoop配置目录 (默认: \$HADOOP_HOME/etc/hadoop)"
    echo "  HADOOP_LOG_DIR             - 日志目录 (默认: \$HADOOP_HOME/logs)"
    echo "  HADOOP_PID_DIR             - PID文件目录 (默认: \$HADOOP_HOME/run)"
    echo "  HADOOP_DATA_DIR            - 数据目录 (默认: \$HADOOP_HOME/data)"
    echo "  HADOOP_TEMP_DIR            - 临时目录 (默认: \$HADOOP_HOME/tmp)"
    echo "  JAVA_HOME                  - Java安装目录"
    echo "  HADOOP_USER                - 运行Hadoop的用户 (默认: hadoop)"
    echo "  HADOOP_GROUP               - 运行Hadoop的用户组 (默认: hadoop)"
    echo "  NAMENODE_PORT              - NameNode Web UI端口 (默认: 9870)"
    echo "  DATANODE_PORT              - DataNode Web UI端口 (默认: 9864)"
    echo "  RESOURCEMANAGER_PORT       - ResourceManager Web UI端口 (默认: 8088)"
    echo "  NODEMANAGER_PORT           - NodeManager Web UI端口 (默认: 8042)"
}

# 主函数
main() {
    local command=$1
    shift || true

    case "$command" in
        start)
            check_environment
            create_directories
            format_namenode "$@"
            start_hdfs
            start_yarn
            start_historyserver
            show_status
            ;;
        stop)
            stop_historyserver
            stop_yarn
            stop_hdfs
            ;;
        status)
            show_status
            ;;
        health)
            check_hdfs_health
            ;;
        format)
            check_environment
            create_directories
            format_namenode "--format"
            ;;
        cleanup)
            cleanup
            ;;
        safemode)
            safemode "$1"
            ;;
        balancer)
            balancer
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