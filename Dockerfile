# 多阶段构建
FROM maven:3.9-eclipse-temurin-17 AS build

# 设置工作目录
WORKDIR /app

# 复制pom文件
COPY pom.xml .
COPY */pom.xml ./

# 复制模块源码
COPY bdirdps-common ./bdirdps-common
COPY bdirdps-dao ./bdirdps-dao
COPY bdirdps-service ./bdirdps-service
COPY bdirdps-web ./bdirdps-web
COPY bdirdps-stream ./bdirdps-stream
COPY bdirdps-batch ./bdirdps-batch

# 构建项目
RUN mvn clean package -DskipTests

# 运行时镜像
FROM eclipse-temurin:17-jre-alpine

# 安装必要的工具
RUN apk add --no-cache \
    curl \
    bash \
    tzdata \
    && ln -snf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone

# 创建应用用户
RUN addgroup -g 1000 bdir && \
    adduser -D -s /bin/bash -u 1000 -G bdir bdir

# 设置工作目录
WORKDIR /app

# 复制构建产物
COPY --from=build /app/bdirdps-web/target/*.jar app.jar
COPY --from=build /app/bdirdps-stream/target/*.jar stream.jar
COPY --from=build /app/bdirdps-batch/target/*.jar batch.jar

# 复制启动脚本
COPY scripts/start-app-docker.sh /app/start-app.sh
COPY scripts/wait-for-it.sh /app/wait-for-it.sh

# 设置权限
RUN chmod +x /app/start-app.sh /app/wait-for-it.sh && \
    chown -R bdir:bdir /app

# 创建日志目录
RUN mkdir -p /app/logs && chown -R bdir:bdir /app/logs

# 切换到应用用户
USER bdir

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/actuator/health || exit 1

# 设置JVM参数
ENV JAVA_OPTS="-Xms512m -Xmx2048m -XX:+UseG1GC -XX:+UseStringDeduplication \
    -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCApplicationStoppedTime \
    -XX:+UseGCOverheadLimit -XX:+DisableExplicitGC -Djava.awt.headless=true \
    -Djava.security.egd=file:/dev/./urandom -Dfile.encoding=UTF-8"

# 暴露端口
EXPOSE 8080 8081

# 启动应用
ENTRYPOINT ["/app/start-app.sh"]