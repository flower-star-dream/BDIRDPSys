-- BDIRDPSys数据库初始化脚本
-- 创建数据库和表结构

-- 创建数据库
CREATE DATABASE IF NOT EXISTS bdir_dps CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE bdir_dps;

-- 1. 机器人维度表
CREATE TABLE IF NOT EXISTS dim_robot (
    robot_id VARCHAR(50) PRIMARY KEY COMMENT '机器人ID',
    robot_name VARCHAR(100) NOT NULL COMMENT '机器人名称',
    robot_type VARCHAR(50) NOT NULL COMMENT '机器人类型（WELDING/ASSEMBLY/TRANSPORT）',
    model VARCHAR(50) COMMENT '型号',
    production_date DATE COMMENT '生产日期',
    location VARCHAR(200) COMMENT '位置',
    department VARCHAR(100) COMMENT '所属部门',
    responsible_user VARCHAR(100) COMMENT '负责人',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    status TINYINT DEFAULT 1 COMMENT '状态（1-正常，0-停用）',
    INDEX idx_type (robot_type),
    INDEX idx_location (location),
    INDEX idx_department (department),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='机器人维度表';

-- 2. 传感器维度表
CREATE TABLE IF NOT EXISTS dim_sensor (
    sensor_id VARCHAR(50) PRIMARY KEY COMMENT '传感器ID',
    sensor_name VARCHAR(100) NOT NULL COMMENT '传感器名称',
    sensor_type VARCHAR(50) NOT NULL COMMENT '传感器类型',
    manufacturer VARCHAR(100) COMMENT '制造商',
    model VARCHAR(50) COMMENT '型号',
    accuracy DECIMAL(5,2) COMMENT '精度',
    measurement_range VARCHAR(100) COMMENT '测量范围',
    calibration_date DATE COMMENT '校准日期',
    status TINYINT DEFAULT 1 COMMENT '状态（1-正常，0-停用）',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_type (sensor_type),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='传感器维度表';

-- 3. 时间维度表
CREATE TABLE IF NOT EXISTS dim_time (
    time_key BIGINT PRIMARY KEY COMMENT '时间键（YYYYMMDDHHMMSS）',
    full_date DATE NOT NULL COMMENT '完整日期',
    year INT NOT NULL COMMENT '年',
    month INT NOT NULL COMMENT '月',
    day INT NOT NULL COMMENT '日',
    hour INT NOT NULL COMMENT '小时',
    minute INT NOT NULL COMMENT '分钟',
    week_of_year INT COMMENT '一年中的第几周',
    day_of_week INT COMMENT '一周中的第几天（1-7）',
    is_weekend BOOLEAN COMMENT '是否周末',
    quarter INT COMMENT '季度',
    INDEX idx_date (full_date),
    INDEX idx_year_month (year, month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='时间维度表';

-- 4. 实时传感器数据表（分区表）
CREATE TABLE IF NOT EXISTS realtime_sensor_data (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    event_time TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) COMMENT '事件时间',
    robot_id VARCHAR(50) NOT NULL COMMENT '机器人ID',
    sensor_id VARCHAR(50) NOT NULL COMMENT '传感器ID',
    sensor_type VARCHAR(50) NOT NULL COMMENT '传感器类型',
    temperature DOUBLE COMMENT '温度值',
    humidity DOUBLE COMMENT '湿度值',
    pressure DOUBLE COMMENT '压力值',
    position_x DOUBLE COMMENT 'X坐标',
    position_y DOUBLE COMMENT 'Y坐标',
    position_z DOUBLE COMMENT 'Z坐标',
    status VARCHAR(20) DEFAULT 'NORMAL' COMMENT '状态',
    INDEX idx_robot_time (robot_id, event_time),
    INDEX idx_sensor_time (sensor_id, event_time),
    INDEX idx_event_time (event_time),
    INDEX idx_sensor_type (sensor_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE (TO_DAYS(event_time)) (
    PARTITION p_current VALUES LESS THAN (TO_DAYS(CURRENT_DATE)),
    PARTITION p_future VALUES LESS THAN MAXVALUE
) COMMENT='实时传感器数据表';

-- 5. 机器人控制指令表
CREATE TABLE IF NOT EXISTS robot_command (
    command_id VARCHAR(50) PRIMARY KEY COMMENT '指令ID',
    robot_id VARCHAR(50) NOT NULL COMMENT '机器人ID',
    command_type VARCHAR(50) NOT NULL COMMENT '指令类型',
    parameters JSON COMMENT '指令参数',
    priority INT DEFAULT 5 COMMENT '优先级',
    status VARCHAR(20) DEFAULT 'PENDING' COMMENT '状态',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    execute_time TIMESTAMP NULL COMMENT '执行时间',
    complete_time TIMESTAMP NULL COMMENT '完成时间',
    error_message TEXT COMMENT '错误信息',
    result TEXT COMMENT '执行结果',
    retry_count INT DEFAULT 0 COMMENT '重试次数',
    max_retry INT DEFAULT 3 COMMENT '最大重试次数',
    timeout INT DEFAULT 30 COMMENT '超时时间（秒）',
    INDEX idx_robot (robot_id),
    INDEX idx_status (status),
    INDEX idx_create_time (create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='机器人控制指令表';

-- 6. 机器人状态表
CREATE TABLE IF NOT EXISTS robot_status (
    robot_id VARCHAR(50) PRIMARY KEY COMMENT '机器人ID',
    robot_name VARCHAR(100) NOT NULL COMMENT '机器人名称',
    robot_type VARCHAR(50) NOT NULL COMMENT '机器人类型',
    status VARCHAR(20) DEFAULT 'OFFLINE' COMMENT '在线状态',
    position_x DOUBLE COMMENT 'X坐标',
    position_y DOUBLE COMMENT 'Y坐标',
    position_z DOUBLE COMMENT 'Z坐标',
    rotation DOUBLE COMMENT '旋转角度',
    task_status VARCHAR(20) DEFAULT 'IDLE' COMMENT '任务状态',
    current_task_id VARCHAR(50) COMMENT '当前任务ID',
    battery_level DOUBLE COMMENT '电池电量',
    temperature DOUBLE COMMENT '温度',
    error_code VARCHAR(50) COMMENT '错误码',
    error_description TEXT COMMENT '错误描述',
    last_update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
    INDEX idx_status (status),
    INDEX idx_task_status (task_status),
    INDEX idx_update_time (last_update_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='机器人状态表';

-- 7. 告警规则表
CREATE TABLE IF NOT EXISTS alert_rule (
    rule_id VARCHAR(50) PRIMARY KEY COMMENT '规则ID',
    rule_name VARCHAR(100) NOT NULL COMMENT '规则名称',
    rule_type VARCHAR(50) NOT NULL COMMENT '规则类型',
    description TEXT COMMENT '规则描述',
    conditions JSON NOT NULL COMMENT '触发条件',
    actions JSON COMMENT '执行动作',
    severity VARCHAR(20) DEFAULT 'MEDIUM' COMMENT '严重级别',
    enable_flag TINYINT DEFAULT 1 COMMENT '启用标志',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_type (rule_type),
    INDEX idx_severity (severity),
    INDEX idx_enable (enable_flag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='告警规则表';

-- 8. 告警记录表
CREATE TABLE IF NOT EXISTS alert_record (
    alert_id VARCHAR(50) PRIMARY KEY COMMENT '告警ID',
    rule_id VARCHAR(50) NOT NULL COMMENT '规则ID',
    alert_type VARCHAR(50) NOT NULL COMMENT '告警类型',
    severity VARCHAR(20) NOT NULL COMMENT '严重级别',
    title VARCHAR(200) NOT NULL COMMENT '告警标题',
    content TEXT COMMENT '告警内容',
    robot_id VARCHAR(50) COMMENT '机器人ID',
    sensor_id VARCHAR(50) COMMENT '传感器ID',
    status VARCHAR(20) DEFAULT 'ACTIVE' COMMENT '告警状态',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_rule (rule_id),
    INDEX idx_robot (robot_id),
    INDEX idx_status (status),
    INDEX idx_create_time (create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='告警记录表';

-- 9. 系统配置表
CREATE TABLE IF NOT EXISTS system_config (
    config_key VARCHAR(100) PRIMARY KEY COMMENT '配置键',
    config_value TEXT NOT NULL COMMENT '配置值',
    config_type VARCHAR(50) DEFAULT 'STRING' COMMENT '配置类型',
    description TEXT COMMENT '配置描述',
    module VARCHAR(50) COMMENT '所属模块',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_module (module)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='系统配置表';

-- 10. 用户表
CREATE TABLE IF NOT EXISTS sys_user (
    user_id VARCHAR(50) PRIMARY KEY COMMENT '用户ID',
    username VARCHAR(50) NOT NULL UNIQUE COMMENT '用户名',
    password VARCHAR(100) NOT NULL COMMENT '密码',
    real_name VARCHAR(100) COMMENT '真实姓名',
    email VARCHAR(100) COMMENT '邮箱',
    phone VARCHAR(20) COMMENT '电话',
    dept_id VARCHAR(50) COMMENT '部门ID',
    status TINYINT DEFAULT 1 COMMENT '状态（1-正常，0-停用）',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_username (username),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表';

-- 11. 角色表
CREATE TABLE IF NOT EXISTS sys_role (
    role_id VARCHAR(50) PRIMARY KEY COMMENT '角色ID',
    role_name VARCHAR(100) NOT NULL COMMENT '角色名称',
    role_code VARCHAR(50) NOT NULL UNIQUE COMMENT '角色编码',
    description TEXT COMMENT '角色描述',
    status TINYINT DEFAULT 1 COMMENT '状态（1-正常，0-停用）',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_code (role_code),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色表';

-- 12. 用户角色关联表
CREATE TABLE IF NOT EXISTS sys_user_role (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    user_id VARCHAR(50) NOT NULL COMMENT '用户ID',
    role_id VARCHAR(50) NOT NULL COMMENT '角色ID',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    UNIQUE KEY uk_user_role (user_id, role_id),
    INDEX idx_user (user_id),
    INDEX idx_role (role_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户角色关联表';

-- 13. 权限表
CREATE TABLE IF NOT EXISTS sys_permission (
    permission_id VARCHAR(50) PRIMARY KEY COMMENT '权限ID',
    permission_name VARCHAR(100) NOT NULL COMMENT '权限名称',
    permission_code VARCHAR(50) NOT NULL UNIQUE COMMENT '权限编码',
    permission_type VARCHAR(20) NOT NULL COMMENT '权限类型',
    parent_id VARCHAR(50) COMMENT '父权限ID',
    path VARCHAR(200) COMMENT '路径',
    component VARCHAR(100) COMMENT '组件',
    icon VARCHAR(50) COMMENT '图标',
    sort_order INT DEFAULT 0 COMMENT '排序',
    status TINYINT DEFAULT 1 COMMENT '状态',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_parent (parent_id),
    INDEX idx_type (permission_type),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='权限表';

-- 14. 角色权限关联表
CREATE TABLE IF NOT EXISTS sys_role_permission (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    role_id VARCHAR(50) NOT NULL COMMENT '角色ID',
    permission_id VARCHAR(50) NOT NULL COMMENT '权限ID',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    UNIQUE KEY uk_role_permission (role_id, permission_id),
    INDEX idx_role (role_id),
    INDEX idx_permission (permission_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色权限关联表';

-- 15. 操作日志表
CREATE TABLE IF NOT EXISTS sys_operation_log (
    log_id VARCHAR(50) PRIMARY KEY COMMENT '日志ID',
    user_id VARCHAR(50) NOT NULL COMMENT '用户ID',
    username VARCHAR(50) NOT NULL COMMENT '用户名',
    operation VARCHAR(100) NOT NULL COMMENT '操作类型',
    method VARCHAR(100) COMMENT '方法名',
    params TEXT COMMENT '请求参数',
    ip VARCHAR(50) COMMENT 'IP地址',
    user_agent TEXT COMMENT '用户代理',
    request_time INT COMMENT '请求耗时（毫秒）',
    result_status INT COMMENT '结果状态',
    error_msg TEXT COMMENT '错误信息',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_user (user_id),
    INDEX idx_operation (operation),
    INDEX idx_create_time (create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='操作日志表';

-- 插入初始数据

-- 插入机器人维度数据
INSERT INTO dim_robot (robot_id, robot_name, robot_type, model, production_date, location, department, responsible_user) VALUES
('R001', '焊接机器人A号', 'WELDING', 'WR-2023-A', '2023-03-15', '车间1-工位A', '焊接车间', '张三'),
('R002', '装配机器人B号', 'ASSEMBLY', 'AR-2023-B', '2023-04-20', '车间2-工位B', '装配车间', '李四'),
('R003', '搬运机器人C号', 'TRANSPORT', 'TR-2023-C', '2023-05-10', '车间3-工位C', '物流车间', '王五');

-- 插入传感器维度数据
INSERT INTO dim_sensor (sensor_id, sensor_name, sensor_type, manufacturer, model, accuracy, measurement_range, calibration_date) VALUES
('S001', '温度传感器1号', 'TEMPERATURE', 'Siemens', 'TS-100', 0.1, '-50~150°C', '2024-01-01'),
('S002', '湿度传感器1号', 'HUMIDITY', 'Honeywell', 'HS-200', 0.5, '0~100%RH', '2024-01-01'),
('S003', '压力传感器1号', 'PRESSURE', 'ABB', 'PS-300', 0.05, '0~10MPa', '2024-01-01'),
('S004', '位置传感器1号', 'POSITION', 'Bosch', 'PS-400', 0.01, '±500mm', '2024-01-01');

-- 插入时间维度数据（示例：2024年1月）
INSERT INTO dim_time (time_key, full_date, year, month, day, hour, minute, week_of_year, day_of_week, is_weekend, quarter)
SELECT
    DATE_FORMAT(dt, '%Y%m%d%H%i%S') as time_key,
    dt as full_date,
    YEAR(dt) as year,
    MONTH(dt) as month,
    DAY(dt) as day,
    HOUR(dt) as hour,
    MINUTE(dt) as minute,
    WEEK(dt, 1) as week_of_year,
    DAYOFWEEK(dt) as day_of_week,
    CASE WHEN DAYOFWEEK(dt) IN (1, 7) THEN 1 ELSE 0 END as is_weekend,
    QUARTER(dt) as quarter
FROM (
    SELECT DATE_ADD('2024-01-01 00:00:00', INTERVAL (a.a + (10 * b.a) + (100 * c.a)) MINUTE) AS dt
    FROM (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS a
    CROSS JOIN (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS b
    CROSS JOIN (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS c
) t
WHERE dt <= '2024-01-31 23:59:59';

-- 插入系统配置数据
INSERT INTO system_config (config_key, config_value, config_type, description, module) VALUES
('sensor.batch.size', '1000', 'INTEGER', '传感器数据批量处理大小', 'sensor'),
('sensor.flush.interval', '5000', 'INTEGER', '传感器数据刷新间隔（毫秒）', 'sensor'),
('robot.command.timeout', '30', 'INTEGER', '机器人指令超时时间（秒）', 'robot'),
('robot.heartbeat.interval', '30000', 'INTEGER', '机器人心跳间隔（毫秒）', 'robot'),
('alert.check.interval', '60000', 'INTEGER', '告警检查间隔（毫秒）', 'alert'),
('data.retention.days', '90', 'INTEGER', '数据保留天数', 'data');

-- 插入告警规则数据
INSERT INTO alert_rule (rule_id, rule_name, rule_type, description, conditions, actions, severity) VALUES
('RULE001', '高温告警', 'TEMPERATURE', '机器人温度超过80度时触发告警',
 '{"metric": "temperature", "operator": "\u003e", "threshold": 80, "duration": 300}',
 '{"type": "webhook", "url": "http://localhost:8080/api/v1/alerts/notify", "method": "POST"}',
 'HIGH'),
('RULE002', '低电量告警', 'BATTERY', '机器人电量低于10%时触发告警',
 '{"metric": "battery_level", "operator": "\u003c", "threshold": 10, "duration": 60}',
 '{"type": "email", "recipients": ["admin@company.com"]}',
 'CRITICAL');

-- 插入用户数据
INSERT INTO sys_user (user_id, username, password, real_name, email, phone, dept_id, status) VALUES
('U001', 'admin', '$2a$10$7JB720yubVSOfvK5j6s0oOjGHGz7l9UjEg3FQJb7g3F3o8JzUoJ9C', '系统管理员', 'admin@company.com', '13800138000', 'DEPT001', 1),
('U002', 'analyst', '$2a$10$7JB720yubVSOfvK5j6s0oOjGHGz7l9UjEg3FQJb7g3F3o8JzUoJ9C', '数据分析师', 'analyst@company.com', '13900139000', 'DEPT002', 1),
('U003', 'operator', '$2a$10$7JB720yubVSOfvK5j6s0oOjGHGz7l9UjEg3FQJb7g3F3o8JzUoJ9C', '操作员', 'operator@company.com', '13700137000', 'DEPT003', 1);

-- 插入角色数据
INSERT INTO sys_role (role_id, role_name, role_code, description) VALUES
('ROLE001', '系统管理员', 'ADMIN', '系统管理员，拥有所有权限'),
('ROLE002', '数据分析师', 'ANALYST', '数据分析师，可以查看和分析数据'),
('ROLE003', '操作员', 'OPERATOR', '操作员，可以控制机器人和查看状态');

-- 插入用户角色关联
INSERT INTO sys_user_role (user_id, role_id) VALUES
('U001', 'ROLE001'),
('U002', 'ROLE002'),
('U003', 'ROLE003');

-- 插入权限数据
INSERT INTO sys_permission (permission_id, permission_name, permission_code, permission_type, path, component, sort_order) VALUES
('PERM001', '系统管理', 'SYSTEM', 'MENU', '/system', 'Layout', 1),
('PERM002', '用户管理', 'USER', 'MENU', '/system/user', 'system/user/index', 2),
('PERM003', '角色管理', 'ROLE', 'MENU', '/system/role', 'system/role/index', 3),
('PERM004', '数据管理', 'DATA', 'MENU', '/data', 'Layout', 4),
('PERM005', '传感器数据', 'SENSOR_DATA', 'MENU', '/data/sensor', 'data/sensor/index', 5),
('PERM006', '机器人控制', 'ROBOT', 'MENU', '/robot', 'Layout', 6),
('PERM007', '机器人列表', 'ROBOT_LIST', 'MENU', '/robot/list', 'robot/list/index', 7),
('PERM008', '告警管理', 'ALERT', 'MENU', '/alert', 'Layout', 8),
('PERM009', '告警规则', 'ALERT_RULE', 'MENU', '/alert/rule', 'alert/rule/index', 9),
('PERM010', '告警记录', 'ALERT_RECORD', 'MENU', '/alert/record', 'alert/record/index', 10);

-- 插入角色权限关联
INSERT INTO sys_role_permission (role_id, permission_id) VALUES
('ROLE001', 'PERM001'), ('ROLE001', 'PERM002'), ('ROLE001', 'PERM003'), ('ROLE001', 'PERM004'), ('ROLE001', 'PERM005'),
('ROLE001', 'PERM006'), ('ROLE001', 'PERM007'), ('ROLE001', 'PERM008'), ('ROLE001', 'PERM009'), ('ROLE001', 'PERM010'),
('ROLE002', 'PERM004'), ('ROLE002', 'PERM005'), ('ROLE002', 'PERM006'), ('ROLE002', 'PERM007'),
('ROLE003', 'PERM006'), ('ROLE003', 'PERM007'), ('ROLE003', 'PERM008'), ('ROLE003', 'PERM009'), ('ROLE003', 'PERM010');

-- 创建Hive表（在Hive中执行）
/*
-- 创建传感器事实表
CREATE TABLE IF NOT EXISTS sensor_fact_orc (
    event_time TIMESTAMP,
    robot_id STRING,
    sensor_type STRING,
    sensor_id STRING,
    temperature DOUBLE,
    humidity DOUBLE,
    pressure DOUBLE,
    position_x DOUBLE,
    position_y DOUBLE,
    position_z DOUBLE,
    status STRING
)
PARTITIONED BY (dt STRING, hr STRING)
CLUSTERED BY (robot_id) SORTED BY (event_time) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES (
    "orc.compress"="ZLIB",
    "orc.create.index"="true"
);

-- 创建小时级聚合表
CREATE TABLE IF NOT EXISTS sensor_hourly_stats (
    dt STRING,
    hr STRING,
    robot_id STRING,
    sensor_type STRING,
    avg_temperature DOUBLE,
    max_temperature DOUBLE,
    min_temperature DOUBLE,
    avg_humidity DOUBLE,
    max_humidity DOUBLE,
    min_humidity DOUBLE,
    avg_pressure DOUBLE,
    max_pressure DOUBLE,
    min_pressure DOUBLE,
    data_count BIGINT,
    abnormal_count BIGINT
)
PARTITIONED BY (dt STRING)
CLUSTERED BY (robot_id) INTO 16 BUCKETS
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
*/

-- 插入机器人状态数据
INSERT INTO robot_status (robot_id, robot_name, robot_type, status, position_x, position_y, position_z, rotation, task_status, current_task_id, battery_level, temperature, last_update_time) VALUES
('R001', '焊接机器人A号', 'WELDING', 'ONLINE', 120.5, 80.3, 45.2, 90.0, 'RUNNING', 'T001', 85.6, 45.2, NOW()),
('R002', '装配机器人B号', 'ASSEMBLY', 'ONLINE', 200.8, 150.6, 60.1, 180.0, 'IDLE', NULL, 92.3, 38.5, NOW()),
('R003', '搬运机器人C号', 'TRANSPORT', 'OFFLINE', 50.2, 300.7, 35.8, 0.0, 'STOPPED', NULL, 15.2, 42.1, NOW());

-- 创建视图
CREATE OR REPLACE VIEW v_robot_sensor_latest AS
SELECT
    r.robot_id,
    r.robot_name,
    r.robot_type,
    r.status,
    r.location,
    r.battery_level,
    r.temperature as robot_temperature,
    s.sensor_id,
    s.sensor_name,
    s.sensor_type,
    t.temperature as sensor_temperature,
    t.humidity,
    t.pressure,
    t.event_time as last_sensor_time
FROM robot_status r
LEFT JOIN realtime_sensor_data t ON r.robot_id = t.robot_id
LEFT JOIN dim_sensor s ON t.sensor_id = s.sensor_id
WHERE t.event_time = (
    SELECT MAX(event_time)
    FROM realtime_sensor_data
    WHERE robot_id = r.robot_id
);

-- 创建存储过程：清理过期数据
DELIMITER //
CREATE PROCEDURE sp_cleanup_old_data(IN days_to_keep INT)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE partition_name VARCHAR(255);
    DECLARE cur CURSOR FOR
        SELECT PARTITION_NAME
        FROM INFORMATION_SCHEMA.PARTITIONS
        WHERE TABLE_SCHEMA = 'bdir_dps'
        AND TABLE_NAME = 'realtime_sensor_data'
        AND PARTITION_NAME != 'p_future'
        AND PARTITION_DESCRIPTION < TO_DAYS(DATE_SUB(CURDATE(), INTERVAL days_to_keep DAY));

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO partition_name;
        IF done THEN
            LEAVE read_loop;
        END IF;

        SET @sql = CONCAT('ALTER TABLE realtime_sensor_data DROP PARTITION ', partition_name);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SELECT CONCAT('Dropped partition: ', partition_name) AS msg;
    END LOOP;
    CLOSE cur;
END //
DELIMITER ;

-- 创建事件：定期清理过期数据（需要开启事件调度器）
/*
SET GLOBAL event_scheduler = ON;

CREATE EVENT IF NOT EXISTS ev_cleanup_old_data
ON SCHEDULE EVERY 1 DAY
STARTS CURRENT_TIMESTAMP
DO
    CALL sp_cleanup_old_data(90);
*/

-- 创建索引优化
CREATE INDEX idx_robot_sensor_time ON realtime_sensor_data(robot_id, sensor_id, event_time);
CREATE INDEX idx_sensor_type_time ON realtime_sensor_data(sensor_type, event_time);

-- 性能监控视图
CREATE OR REPLACE VIEW v_system_performance AS
SELECT
    'realtime_sensor_data' as table_name,
    COUNT(*) as total_records,
    MIN(event_time) as earliest_time,
    MAX(event_time) as latest_time,
    COUNT(DISTINCT robot_id) as unique_robots,
    COUNT(DISTINCT sensor_id) as unique_sensors,
    ROUND(AVG(temperature), 2) as avg_temperature,
    ROUND(AVG(humidity), 2) as avg_humidity,
    ROUND(AVG(pressure), 2) as avg_pressure
FROM realtime_sensor_data
WHERE event_time >= DATE_SUB(NOW(), INTERVAL 1 DAY);

-- 权限设置
GRANT SELECT, INSERT, UPDATE, DELETE ON bdir_dps.* TO 'bdir_user'@'localhost';
FLUSH PRIVILEGES;