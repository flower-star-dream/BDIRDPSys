#!/bin/bash

# BDIRDPSys应用程序启动脚本
# 用于启动Spring Boot应用程序

set -e

# 配置变量 - 使用环境变量或默认值
APP_NAME="${APP_NAME:-BDIRDPSys}"
APP_JAR="${APP_JAR:-bdirdps-web-1.0.0.jar}"
APP_HOME="${APP_HOME:-$(cd "$(dirname "$0")/.." && pwd)}"
APP_LOG_DIR="${APP_LOG_DIR:-$APP_HOME/logs}"
APP_PID_DIR="${APP_PID_DIR:-$APP_HOME/run}"
APP_TEMP_DIR="${APP_TEMP_DIR:-$APP_HOME/temp}"
APP_DATA_DIR="${APP_DATA_DIR:-$APP_HOME/data}"

# Java和JVM配置
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk}"
JAVA_OPTS="${JAVA_OPTS:-"-Xms1g -Xmx4g -XX:+UseG1GC -XX:+UseStringDeduplication"}"
SPRING_PROFILES_ACTIVE="${SPRING_PROFILES_ACTIVE:-"prod"}"

# 外部服务配置 - 从环境变量读取
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-3306}"
DB_NAME="${DB_NAME:-bdir_dps}"
DB_USERNAME="${DB_USERNAME:-bdir_user}"
DB_PASSWORD="${DB_PASSWORD:-}"

KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
HIVE_URL="${HIVE_URL:-jdbc:hive2://localhost:10000/default}"

# JMX配置
JMX_PORT="${JMX_PORT:-}"
JMX_HOSTNAME="${JMX_HOSTNAME:-localhost}"

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
    if [ -n "$JAVA_HOME" ]; then
        JAVA_CMD="$JAVA_HOME/bin/java"
    else
        JAVA_CMD="java"
    fi

    if ! command -v $JAVA_CMD > /dev/null 2>&1; then
        log_error "Java未安装或未配置环境变量"
        exit 1
    fi

    # 检查Java版本
    local java_version=$($JAVA_CMD -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$java_version" -lt 17 ]; then
        log_error "需要Java 17或更高版本，当前版本: $java_version"
        exit 1
    fi

    # 检查Maven（如果需要构建）
    if [ "$1" = "--build" ]; then
        if ! command -v mvn > /dev/null 2>&1; then
            log_error "Maven未安装或未配置环境变量"
            exit 1
        fi
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

    # 检查外部服务
    check_external_services

    log_info "环境检查通过"
}

# 检查外部服务
check_external_services() {
    log_info "检查外部服务连接..."

    # 检查MySQL
    if command -v nc > /dev/null 2>&1; then
        if ! nc -z "$DB_HOST" "$DB_PORT" 2>/dev/null; then
            log_warn "无法连接到MySQL服务: $DB_HOST:$DB_PORT"
        else
            log_info "MySQL服务正常: $DB_HOST:$DB_PORT"
        fi
    fi

    # 检查Kafka
    if command -v nc > /dev/null 2>&1; then
        local kafka_host=$(echo "$KAFKA_BOOTSTRAP_SERVERS" | cut -d':' -f1)
        local kafka_port=$(echo "$KAFKA_BOOTSTRAP_SERVERS" | cut -d':' -f2)
        if ! nc -z "$kafka_host" "$kafka_port" 2>/dev/null; then
            log_warn "无法连接到Kafka服务: $KAFKA_BOOTSTRAP_SERVERS"
        else
            log_info "Kafka服务正常: $KAFKA_BOOTSTRAP_SERVERS"
        fi
    fi

    # 检查Redis
    if command -v redis-cli > /dev/null 2>&1; then
        if ! redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping > /dev/null 2>&1; then
            log_warn "无法连接到Redis服务: $REDIS_HOST:$REDIS_PORT"
        else
            log_info "Redis服务正常: $REDIS_HOST:$REDIS_PORT"
        fi
    fi
}

# 创建必要的目录
create_directories() {
    log_info "创建必要的目录..."

    # 创建目录
    mkdir -p "$APP_LOG_DIR"
    mkdir -p "$APP_PID_DIR"
    mkdir -p "$APP_TEMP_DIR"
    mkdir -p "$APP_DATA_DIR"
    mkdir -p "$APP_HOME/config"
    mkdir -p "$APP_HOME/backup"

    # 设置权限
    chmod 755 "$APP_LOG_DIR"
    chmod 755 "$APP_PID_DIR"
    chmod 755 "$APP_TEMP_DIR"
    chmod 755 "$APP_DATA_DIR"

    log_info "目录创建完成"
}

# 构建应用程序（如果需要）
build_application() {
    if [ "$1" = "--build" ]; then
        log_info "构建应用程序..."

        if [ -f "$APP_HOME/pom.xml" ]; then
            cd "$APP_HOME"
            mvn clean package -DskipTests -Dspring.profiles.active="$SPRING_PROFILES_ACTIVE"
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
    JVM_OPTS="$JVM_OPTS -Djava.net.preferIPv4Stack=true"

    # 内存参数
    JVM_OPTS="$JVM_OPTS -Xms1g -Xmx4g"
    JVM_OPTS="$JVM_OPTS -XX:MetaspaceSize=256m"
    JVM_OPTS="$JVM_OPTS -XX:MaxMetaspaceSize=512m"

    # GC参数
    JVM_OPTS="$JVM_OPTS -XX:+UseG1GC"
    JVM_OPTS="$JVM_OPTS -XX:MaxGCPauseMillis=200"
    JVM_OPTS="$JVM_OPTS -XX:G1HeapRegionSize=16m"
    JVM_OPTS="$JVM_OPTS -XX:+UseStringDeduplication"
    JVM_OPTS="$JVM_OPTS -XX:+ParallelRefProcEnabled"
    JVM_OPTS="$JVM_OPTS -XX:+UseLargePages"

    # GC日志
    if [ ! -d "$APP_LOG_DIR" ]; then
        mkdir -p "$APP_LOG_DIR"
    fi
    JVM_OPTS="$JVM_OPTS -Xlog:gc:$APP_LOG_DIR/gc.log"
    JVM_OPTS="$JVM_OPTS -Xlog:gc*:$APP_LOG_DIR/gc-details.log:time,uptime,level,tags"

    # 调试参数（可选）
    if [ -n "$DEBUG_PORT" ]; then
        JVM_OPTS="$JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:$DEBUG_PORT"
        log_info "启用调试模式，端口: $DEBUG_PORT"
    fi

    # JMX参数（可选）
    if [ -n "$JMX_PORT" ]; then
        JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote"
        JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT"
        JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.host=$JMX_HOSTNAME"
        JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
        JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl=false"
        JVM_OPTS="$JVM_OPTS -Djava.rmi.server.hostname=$JMX_HOSTNAME"
    fi

    # 系统属性
    JVM_OPTS="$JVM_OPTS -Dapp.home=$APP_HOME"
    JVM_OPTS="$JVM_OPTS -Dapp.name=$APP_NAME"
    JVM_OPTS="$JVM_OPTS -Dapp.pid=$$"
    JVM_OPTS="$JVM_OPTS -Dapp.temp=$APP_TEMP_DIR"
    JVM_OPTS="$JVM_OPTS -Dapp.data=$APP_DATA_DIR"

    # 应用程序参数
    APP_OPTS="--spring.profiles.active=$SPRING_PROFILES_ACTIVE"
    APP_OPTS="$APP_OPTS --logging.file.path=$APP_LOG_DIR"
    APP_OPTS="$APP_OPTS --logging.file.name=$APP_NAME.log"
    APP_OPTS="$APP_OPTS --server.tomcat.basedir=$APP_TEMP_DIR"

    # 数据库配置
    APP_OPTS="$APP_OPTS --spring.datasource.url=jdbc:mysql://$DB_HOST:$DB_PORT/$DB_NAME?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai"
    APP_OPTS="$APP_OPTS --spring.datasource.username=$DB_USERNAME"
    if [ -n "$DB_PASSWORD" ]; then
        APP_OPTS="$APP_OPTS --spring.datasource.password=$DB_PASSWORD"
    fi

    # Redis配置
    APP_OPTS="$APP_OPTS --spring.redis.host=$REDIS_HOST"
    APP_OPTS="$APP_OPTS --spring.redis.port=$REDIS_PORT"

    # Kafka配置
    APP_OPTS="$APP_OPTS --spring.kafka.bootstrap-servers=$KAFKA_BOOTSTRAP_SERVERS"

    # Hive配置
    APP_OPTS="$APP_OPTS --hive.url=$HIVE_URL"

    log_info "JVM参数设置完成"
    log_debug "JVM_OPTS: $JVM_OPTS"
    log_debug "APP_OPTS: $APP_OPTS"
}

# 检查端口是否被占用
check_port() {
    local port=$1
    local service_name=${2:-"服务"}

    if command -v lsof > /dev/null 2>&1; then
        if lsof -i :$port > /dev/null 2>&1; then
            log_error "$service_name 端口 $port 已被占用"
            exit 1
        fi
    elif command -v netstat > /dev/null 2>&1; then
        if netstat -tuln 2>/dev/null | grep -q ":$port "; then
            log_error "$service_name 端口 $port 已被占用"
            exit 1
        fi
    fi
}

# 启动应用程序
start_application() {
    log_info "启动 $APP_NAME 应用程序..."

    # 检查端口
    check_port 8080 "Web"
    check_port 8081 "Actuator"
    check_port 9090 "WebSocket"

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
    local pid_file="$APP_PID_DIR/$APP_NAME.pid"

    nohup $JAVA_CMD $JVM_OPTS \
        -jar "$APP_HOME/$APP_JAR" \
        $APP_OPTS \
        > "$log_file" 2>&1 &

    local app_pid=$!
    echo $app_pid > "$pid_file"

    log_info "$APP_NAME 已启动 (PID: $app_pid)"
    log_info "日志文件: $log_file"
    log_info "PID文件: $pid_file"

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
    echo "  数据目录: $APP_DATA_DIR"
    echo "  临时目录: $APP_TEMP_DIR"
    echo "  PID文件: $APP_PID_DIR/$APP_NAME.pid"

    if [ -n "$JMX_PORT" ]; then
        echo "  JMX端口: $JMX_PORT"
        echo "  JMX主机: $JMX_HOSTNAME"
    fi

    if [ -n "$DEBUG_PORT" ]; then
        echo "  调试端口: $DEBUG_PORT"
    fi

    # 显示连接的外部服务
    log_info "外部服务连接:"
    echo "  MySQL: $DB_HOST:$DB_PORT/$DB_NAME"
    echo "  Kafka: $KAFKA_BOOTSTRAP_SERVERS"
    echo "  Hive: $HIVE_URL"
    echo "  Redis: $REDIS_HOST:$REDIS_PORT"
}

# 停止应用程序
stop_application() {
    log_info "停止 $APP_NAME 应用程序..."

    local pid_file="$APP_PID_DIR/$APP_NAME.pid"

    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p $pid > /dev/null 2>&1; then
            log_info "发送终止信号给进程 $pid..."
            kill -TERM $pid

            # 等待进程停止
            local max_wait=30
            local wait_count=0
            while [ $wait_count -lt $max_wait ]; do
                if ! ps -p $pid > /dev/null 2>&1; then
                    log_info "$APP_NAME 已停止"
                    rm -f "$pid_file"
                    return 0
                fi
                sleep 1
                wait_count=$((wait_count + 1))
            done

            # 强制终止
            log_warn "强制终止进程 $pid..."
            kill -9 $pid
            rm -f "$pid_file"
        else
            log_warn "进程 $pid 不存在"
            rm -f "$pid_file"
        fi
    else
        log_warn "PID文件不存在，尝试查找进程..."
        local pid=$(jps -l | grep "$APP_JAR" | awk '{print $1}')
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
    local pid_file="$APP_PID_DIR/$APP_NAME.pid"

    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
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

            # 显示线程数
            local thread_count=$(ps -p $pid -o nlwp= 2>/dev/null | tr -d ' ')
            if [ -n "$thread_count" ]; then
                log_info "线程数: $thread_count"
            fi

            return 0
        else
            log_warn "PID文件存在但进程不存在"
            rm -f "$pid_file"
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
    local log_file=$(ls -t "$APP_LOG_DIR"/*.log 2>/dev/null | head -n 1)
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
    if [ -d "$APP_TEMP_DIR" ]; then
        find "$APP_TEMP_DIR" -type f -mtime +1 -delete 2>/dev/null || true
    fi

    # 清理空目录
    find "$APP_LOG_DIR" -type d -empty -delete 2>/dev/null || true
    find "$APP_TEMP_DIR" -type d -empty -delete 2>/dev/null || true

    log_info "清理完成"
}

# 生成systemd服务文件
generate_systemd_service() {
    local service_file="/etc/systemd/system/bdir-dps.service"

    log_info "生成systemd服务文件: $service_file"

    sudo tee "$service_file" > /dev/null <<EOF
[Unit]
Description=BDIRDPSys Application
Documentation=https://github.com/bdir/dps
Requires=network.target
After=network.target

[Service]
Type=forking
User=$USER
Group=$USER
WorkingDirectory=$APP_HOME
Environment=APP_HOME=$APP_HOME
Environment=JAVA_HOME=$JAVA_HOME
Environment=JAVA_OPTS=$JAVA_OPTS
Environment=SPRING_PROFILES_ACTIVE=$SPRING_PROFILES_ACTIVE
Environment=DB_HOST=$DB_HOST
Environment=DB_PORT=$DB_PORT
Environment=DB_NAME=$DB_NAME
Environment=DB_USERNAME=$DB_USERNAME
Environment=DB_PASSWORD=$DB_PASSWORD
Environment=KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS
Environment=REDIS_HOST=$REDIS_HOST
Environment=REDIS_PORT=$REDIS_PORT
Environment=HIVE_URL=$HIVE_URL
ExecStart=$APP_HOME/scripts/start-app.sh start
ExecStop=$APP_HOME/scripts/start-app.sh stop
ExecReload=$APP_HOME/scripts/start-app.sh restart
PIDFile=$APP_PID_DIR/$APP_NAME.pid
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$APP_NAME

[Install]
WantedBy=multi-user.target
EOF

    sudo systemctl daemon-reload
    log_info "systemd服务文件已生成，可以使用以下命令管理服务:"
    echo "  sudo systemctl start bdir-dps"
    echo "  sudo systemctl stop bdir-dps"
    echo "  sudo systemctl status bdir-dps"
    echo "  sudo systemctl enable bdir-dps  # 开机自启"
}

# 显示帮助信息
show_help() {
    echo "Usage: $0 {start|stop|status|restart|logs|cleanup|systemd|help} [--build]"
    echo ""
    echo "命令:"
    echo "  start    - 启动应用程序"
    echo "  stop     - 停止应用程序"
    echo "  status   - 检查应用程序状态"
    echo "  restart  - 重启应用程序"
    echo "  logs     - 查看实时日志"
    echo "  cleanup  - 清理日志和临时文件"
    echo "  systemd  - 生成systemd服务文件"
    echo "  help     - 显示帮助信息"
    echo ""
    echo "选项:"
    echo "  --build  - 在启动前构建应用程序（需要Maven）"
    echo ""
    echo "环境变量:"
    echo "  APP_NAME                   - 应用程序名称 (默认: BDIRDPSys)"
    echo "  APP_JAR                    - JAR文件名 (默认: bdirdps-web-1.0.0.jar)"
    echo "  APP_HOME                   - 应用主目录 (默认: 脚本所在目录的父目录)"
    echo "  APP_LOG_DIR                - 日志目录 (默认: \$APP_HOME/logs)"
    echo "  APP_PID_DIR                - PID文件目录 (默认: \$APP_HOME/run)"
    echo "  APP_TEMP_DIR               - 临时目录 (默认: \$APP_HOME/temp)"
    echo "  APP_DATA_DIR               - 数据目录 (默认: \$APP_HOME/data)"
    echo "  JAVA_HOME                  - Java安装目录"
    echo "  JAVA_OPTS                  - JVM参数"
    echo "  SPRING_PROFILES_ACTIVE     - Spring Profile (默认: prod)"
    echo "  DEBUG_PORT                 - 调试端口（启用调试模式）"
    echo "  JMX_PORT                   - JMX端口"
    echo "  JMX_HOSTNAME               - JMX主机名 (默认: localhost)"
    echo "  DB_HOST                    - 数据库主机 (默认: localhost)"
    echo "  DB_PORT                    - 数据库端口 (默认: 3306)"
    echo "  DB_NAME                    - 数据库名称 (默认: bdir_dps)"
    echo "  DB_USERNAME                - 数据库用户名 (默认: bdir_user)"
    echo "  DB_PASSWORD                - 数据库密码"
    echo "  KAFKA_BOOTSTRAP_SERVERS    - Kafka服务器地址 (默认: localhost:9092)"
    echo "  REDIS_HOST                 - Redis主机 (默认: localhost)"
    echo "  REDIS_PORT                 - Redis端口 (默认: 6379)"
    echo "  HIVE_URL                   - Hive连接URL (默认: jdbc:hive2://localhost:10000/default)"
}

# 显示版本信息
show_version() {
    log_info "BDIRDPSys 启动脚本版本 2.0"
    log_info "应用主目录: $APP_HOME"
    log_info "Java版本: $($JAVA_CMD -version 2>&1 | head -n1)"
    log_info "Maven版本: $(mvn -version 2>/dev/null | head -n1 || echo '未安装')"
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
        systemd)
            generate_systemd_service
            ;;
        version|--version|-v)
            show_version
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