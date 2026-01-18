#!/bin/bash

# BDIRDPSys应用程序启动脚本
# 用于启动Spring Boot应用程序

set -e

# 配置变量
APP_NAME="BDIRDPSys"
APP_JAR="bdirdps-web-1.0.0.jar"
APP_HOME=$(cd "$(dirname "$0")/.." && pwd)
APP_LOG_DIR=${APP_LOG_DIR:-/var/log/bdir-dps}
APP_PID_DIR=${APP_PID_DIR:-/var/run/bdir-dps}
JAVA_OPTS=${JAVA_OPTS:-"-Xms1g -Xmx4g -XX:+UseG1GC -XX:+UseStringDeduplication"}
SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE:-"prod"}

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
    log_info "检查应用程序环境..."

    # 检查Java环境
    if ! command -v java > /dev/null 2>&1; then
        log_error "Java未安装或未配置环境变量"
        exit 1
    fi

    # 检查Java版本
    local java_version=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$java_version" -lt 17 ]; then
        log_error "需要Java 17或更高版本，当前版本: $java_version"
        exit 1
    fi

    # 检查应用程序JAR文件
    if [ ! -f "$APP_HOME/$APP_JAR" ]; then
        log_error "应用程序JAR文件不存在: $APP_HOME/$APP_JAR"
        exit 1
    fi

    # 检查配置文件
    if [ ! -f "$APP_HOME/config/application.yml" ] && [ ! -f "$APP_HOME/config/application.properties" ]; then
        log_warn "未找到配置文件，将使用默认配置"
    fi

    log_info "环境检查通过"
}

# 创建必要的目录
create_directories() {
    log_info "创建必要的目录..."

    # 创建日志目录
    mkdir -p "$APP_LOG_DIR"
    mkdir -p "$APP_PID_DIR"
    mkdir -p "$APP_HOME/temp"
    mkdir -p "$APP_HOME/data"

    # 设置权限
    chmod 755 "$APP_LOG_DIR"
    chmod 755 "$APP_PID_DIR"

    log_info "目录创建完成"
}

# 构建应用程序（如果需要）
build_application() {
    if [ "$1" = "--build" ]; then
        log_info "构建应用程序..."

        if [ -f "$APP_HOME/pom.xml" ]; then
            cd "$APP_HOME"
            mvn clean package -DskipTests
            if [ $? -ne 0 ]; then
                log_error "构建失败"
                exit 1
            fi
            log_info "构建完成"
        else
            log_warn "未找到pom.xml文件，跳过构建"
        fi
    fi
}

# 设置JVM参数
setup_jvm_params() {
    log_info "设置JVM参数..."

    # 基础JVM参数
    JVM_OPTS="$JAVA_OPTS"
    JVM_OPTS="$JVM_OPTS -server"
    JVM_OPTS="$JVM_OPTS -Dfile.encoding=UTF-8"
    JVM_OPTS="$JVM_OPTS -Djava.awt.headless=true"

    # 内存参数
    JVM_OPTS="$JVM_OPTS -Xms1g -Xmx4g"
    JVM_OPTS="$JVM_OPTS -XX:MetaspaceSize=256m"
    JVM_OPTS="$JVM_OPTS -XX:MaxMetaspaceSize=512m"

    # GC参数
    JVM_OPTS="$JVM_OPTS -XX:+UseG1GC"
    JVM_OPTS="$JVM_OPTS -XX:MaxGCPauseMillis=200"
    JVM_OPTS="$JVM_OPTS -XX:G1HeapRegionSize=16m"
    JVM_OPTS="$JVM_OPTS -XX:+UseStringDeduplication"

    # GC日志
    JVM_OPTS="$JVM_OPTS -Xlog:gc:$APP_LOG_DIR/gc.log"
    JVM_OPTS="$JVM_OPTS -Xlog:gc*:$APP_LOG_DIR/gc-details.log"

    # JMX参数（可选）
    if [ -n "$JMX_PORT" ]; then
        JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote"
        JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT"
        JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
        JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl=false"
    fi

    # 应用程序参数
    APP_OPTS="--spring.profiles.active=$SPRING_PROFILES_ACTIVE"
    APP_OPTS="$APP_OPTS --logging.file.path=$APP_LOG_DIR"
    APP_OPTS="$APP_OPTS --logging.file.name=$APP_NAME.log"
    APP_OPTS="$APP_OPTS --server.tomcat.basedir=$APP_HOME/temp"

    # 数据库配置（如果通过环境变量传递）
    if [ -n "$DB_HOST" ]; then
        APP_OPTS="$APP_OPTS --spring.datasource.url=jdbc:mysql://$DB_HOST:3306/bdir_dps"
    fi
    if [ -n "$DB_USERNAME" ]; then
        APP_OPTS="$APP_OPTS --spring.datasource.username=$DB_USERNAME"
    fi
    if [ -n "$DB_PASSWORD" ]; then
        APP_OPTS="$APP_OPTS --spring.datasource.password=$DB_PASSWORD"
    fi

    # Kafka配置（如果通过环境变量传递）
    if [ -n "$KAFKA_BOOTSTRAP_SERVERS" ]; then
        APP_OPTS="$APP_OPTS --spring.kafka.bootstrap-servers=$KAFKA_BOOTSTRAP_SERVERS"
    fi

    # Hive配置（如果通过环境变量传递）
    if [ -n "$HIVE_URL" ]; then
        APP_OPTS="$APP_OPTS --hive.url=$HIVE_URL"
    fi

    log_info "JVM参数设置完成"
    log_debug "JVM_OPTS: $JVM_OPTS"
    log_debug "APP_OPTS: $APP_OPTS"
}

# 检查端口是否被占用
check_port() {
    local port=$1
    if netstat -tuln 2>/dev/null | grep -q ":$port "; then
        log_error "端口 $port 已被占用"
        exit 1
    fi
}

# 启动应用程序
start_application() {
    log_info "启动 $APP_NAME 应用程序..."

    # 检查端口
    check_port 8080  # Web端口
    check_port 8081  # Actuator端口
    check_port 9090  # WebSocket端口

    # 检查是否已在运行
    if [ -f "$APP_PID_DIR/$APP_NAME.pid" ]; then
        local pid=$(cat "$APP_PID_DIR/$APP_NAME.pid")
        if ps -p $pid > /dev/null 2>&1; then
            log_error "$APP_NAME 已在运行 (PID: $pid)"
            exit 1
        else
            rm -f "$APP_PID_DIR/$APP_NAME.pid"
        fi
    fi

    # 设置内存映射文件限制
    ulimit -n 65536

    # 启动应用程序
    local log_file="$APP_LOG_DIR/$APP_NAME-$(date +%Y%m%d-%H%M%S).log"

    nohup java $JVM_OPTS \
        -jar "$APP_HOME/$APP_JAR" \
        $APP_OPTS \
        > "$log_file" 2>&1 &

    local app_pid=$!
    echo $app_pid > "$APP_PID_DIR/$APP_NAME.pid"

    log_info "$APP_NAME 已启动 (PID: $app_pid)"
    log_info "日志文件: $log_file"

    # 等待应用程序启动
    local max_wait=60
    local wait_count=0

    log_info "等待应用程序启动..."
    while [ $wait_count -lt $max_wait ]; do
        if curl -s http://localhost:8080/actuator/health > /dev/null 2>&1; then
            log_info "$APP_NAME 启动成功！"
            show_application_info
            return 0
        fi
        sleep 2
        wait_count=$((wait_count + 2))
        log_debug "等待启动... ($wait_count/$max_wait)"
    done

    log_error "$APP_NAME 启动超时"
    return 1
}

# 显示应用程序信息
show_application_info() {
    log_info "应用程序信息:"
    echo "  应用名称: $APP_NAME"
    echo "  应用主页: http://localhost:8080/"
    echo "  健康检查: http://localhost:8080/actuator/health"
    echo "  API文档: http://localhost:8080/swagger-ui.html"
    echo "  指标监控: http://localhost:8080/actuator/metrics"
    echo "  WebSocket端点: ws://localhost:9090/ws"
    echo "  日志目录: $APP_LOG_DIR"
    echo "  数据目录: $APP_HOME/data"

    if [ -n "$JMX_PORT" ]; then
        echo "  JMX端口: $JMX_PORT"
    fi

    # 显示连接的外部服务
    log_info "外部服务连接:"
    echo "  MySQL: ${DB_HOST:-localhost}:3306"
    echo "  Kafka: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
    echo "  Hive: ${HIVE_URL:-jdbc:hive2://localhost:10000/default}"
    echo "  Redis: ${REDIS_HOST:-localhost}:6379"
}

# 停止应用程序
stop_application() {
    log_info "停止 $APP_NAME 应用程序..."

    if [ -f "$APP_PID_DIR/$APP_NAME.pid" ]; then
        local pid=$(cat "$APP_PID_DIR/$APP_NAME.pid")
        if ps -p $pid > /dev/null 2>&1; then
            log_info "发送终止信号给进程 $pid..."
            kill -TERM $pid

            # 等待进程停止
            local max_wait=30
            local wait_count=0
            while [ $wait_count -lt $max_wait ]; do
                if ! ps -p $pid > /dev/null 2>&1; then
                    log_info "$APP_NAME 已停止"
                    rm -f "$APP_PID_DIR/$APP_NAME.pid"
                    return 0
                fi
                sleep 1
                wait_count=$((wait_count + 1))
            done

            # 强制终止
            log_warn "强制终止进程 $pid..."
            kill -9 $pid
            rm -f "$APP_PID_DIR/$APP_NAME.pid"
        else
            log_warn "进程 $pid 不存在"
            rm -f "$APP_PID_DIR/$APP_NAME.pid"
        fi
    else
        log_warn "PID文件不存在，尝试查找进程..."
        local pid=$(jps | grep -i "$APP_NAME" | awk '{print $1}')
        if [ -n "$pid" ]; then
            log_info "找到进程 $pid，正在终止..."
            kill -TERM $pid
            sleep 5
            if ps -p $pid > /dev/null 2>&1; then
                kill -9 $pid
            fi
        else
            log_warn "未找到运行中的 $APP_NAME 进程"
        fi
    fi
}

# 检查应用程序状态
check_status() {
    if [ -f "$APP_PID_DIR/$APP_NAME.pid" ]; then
        local pid=$(cat "$APP_PID_DIR/$APP_NAME.pid")
        if ps -p $pid > /dev/null 2>&1; then
            log_info "$APP_NAME 正在运行 (PID: $pid)"

            # 检查健康状态
            local health_status=$(curl -s http://localhost:8080/actuator/health 2>/dev/null)
            if [ -n "$health_status" ]; then
                log_info "健康状态: $health_status"
            fi

            # 显示内存使用情况
            local mem_usage=$(ps -p $pid -o %mem= 2>/dev/null | tr -d ' ')
            if [ -n "$mem_usage" ]; then
                log_info "内存使用率: ${mem_usage}%"
            fi

            return 0
        else
            log_warn "PID文件存在但进程不存在"
            rm -f "$APP_PID_DIR/$APP_NAME.pid"
            return 1
        fi
    else
        log_warn "$APP_NAME 未运行"
        return 1
    fi
}

# 重启应用程序
restart_application() {
    log_info "重启 $APP_NAME 应用程序..."
    stop_application
    sleep 5
    start_application
}

# 查看日志
tail_logs() {
    local log_file=$(ls -t "$APP_LOG_DIR"/*.log | head -n 1)
    if [ -n "$log_file" ]; then
        log_info "查看日志文件: $log_file"
        tail -f "$log_file"
    else
        log_error "未找到日志文件"
    fi
}

# 清理日志和临时文件
cleanup() {
    log_info "清理日志和临时文件..."

    # 清理旧的日志文件（保留最近30天）
    find "$APP_LOG_DIR" -name "*.log" -type f -mtime +30 -delete 2>/dev/null || true
    find "$APP_LOG_DIR" -name "gc*.log" -type f -mtime +7 -delete 2>/dev/null || true

    # 清理临时目录
    rm -rf "$APP_HOME/temp/*"

    log_info "清理完成"
}

# 显示帮助信息
show_help() {
    echo "Usage: $0 {start|stop|status|restart|logs|cleanup|help} [--build]"
    echo ""
    echo "命令:"
    echo "  start    - 启动应用程序"
    echo "  stop     - 停止应用程序"
    echo "  status   - 检查应用程序状态"
    echo "  restart  - 重启应用程序"
    echo "  logs     - 查看实时日志"
    echo "  cleanup  - 清理日志和临时文件"
    echo "  help     - 显示帮助信息"
    echo ""
    echo "选项:"
    echo "  --build  - 在启动前构建应用程序（需要Maven）"
    echo ""
    echo "环境变量:"
    echo "  JAVA_OPTS                  - JVM参数"
    echo "  SPRING_PROFILES_ACTIVE     - Spring Profile (默认: prod)"
    echo "  DB_HOST                    - 数据库主机"
    echo "  DB_USERNAME                - 数据库用户名"
    echo "  DB_PASSWORD                - 数据库密码"
    echo "  KAFKA_BOOTSTRAP_SERVERS    - Kafka服务器地址"
    echo "  HIVE_URL                   - Hive连接URL"
    echo "  REDIS_HOST                 - Redis主机"
    echo "  JMX_PORT                   - JMX端口"
    echo "  APP_LOG_DIR                - 日志目录"
    echo "  APP_PID_DIR                - PID文件目录"
}

# 主函数
main() {
    local command=$1
    shift || true

    case "$command" in
        start)
            build_application "$@"
            check_environment
            create_directories
            setup_jvm_params
            start_application
            ;;
        stop)
            stop_application
            ;;
        status)
            check_status
            ;;
        restart)
            restart_application
            ;;
        logs)
            tail_logs
            ;;
        cleanup)
            cleanup
            ;;
        help|--help|-h)
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

# Systemd服务文件（可选）
# 保存为 /etc/systemd/system/bdir-dps.service
: '
[Unit]
Description=BDIRDPSys Application
Requires=network.target mysql.service kafka.service
After=network.target mysql.service kafka.service

[Service]
Type=forking
User=app
Group=app
Environment=APP_HOME=/opt/bdir-dps
Environment=JAVA_OPTS=-Xms1g -Xmx4g -XX:+UseG1GC
Environment=SPRING_PROFILES_ACTIVE=prod
Environment=DB_HOST=localhost
Environment=KAFKA_BOOTSTRAP_SERVERS=localhost:9092
Environment=HIVE_URL=jdbc:hive2://localhost:10000/default
ExecStart=/opt/bdir-dps/scripts/start-app.sh start
ExecStop=/opt/bdir-dps/scripts/start-app.sh stop
ExecReload=/opt/bdir-dps/scripts/start-app.sh restart
PIDFile=/var/run/bdir-dps/BDIRDPSys.pid
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
'