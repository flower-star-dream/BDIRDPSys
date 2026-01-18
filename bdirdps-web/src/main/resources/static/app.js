// 应用主逻辑
class BDIRDPApp {
    constructor() {
        this.ws = null;
        this.charts = {};
        this.selectedRobotId = null;
        this.apiBase = '/api';
        this.init();
    }

    init() {
        this.initCharts();
        this.initWebSocket();
        this.startDataUpdate();
        this.loadRobots();
    }

    // 初始化图表
    initCharts() {
        // 实时数据流量图
        this.charts.realtime = echarts.init(document.getElementById('realtimeChart'));
        const realtimeOption = {
            title: {
                text: '数据流量 (条/秒)',
                textStyle: { fontSize: 14 }
            },
            tooltip: {
                trigger: 'axis'
            },
            xAxis: {
                type: 'category',
                data: this.generateTimeLabels(20),
                axisLabel: { fontSize: 10 }
            },
            yAxis: {
                type: 'value',
                axisLabel: { fontSize: 10 }
            },
            series: [{
                name: '数据流量',
                type: 'line',
                data: new Array(20).fill(0),
                smooth: true,
                areaStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: 'rgba(102, 126, 234, 0.5)' },
                        { offset: 1, color: 'rgba(102, 126, 234, 0.1)' }
                    ])
                },
                lineStyle: { color: '#667eea' }
            }]
        };
        this.charts.realtime.setOption(realtimeOption);

        // 传感器类型分布图
        this.charts.sensorType = echarts.init(document.getElementById('sensorTypeChart'));
        const sensorTypeOption = {
            title: {
                text: '传感器类型分布',
                textStyle: { fontSize: 14 }
            },
            tooltip: {
                trigger: 'item'
            },
            series: [{
                type: 'pie',
                radius: ['40%', '70%'],
                data: [
                    { value: 335, name: '温度' },
                    { value: 310, name: '湿度' },
                    { value: 234, name: '压力' },
                    { value: 135, name: '位置' },
                    { value: 148, name: '视觉' }
                ],
                emphasis: {
                    itemStyle: {
                        shadowBlur: 10,
                        shadowOffsetX: 0,
                        shadowColor: 'rgba(0, 0, 0, 0.5)'
                    }
                }
            }]
        };
        this.charts.sensorType.setOption(sensorTypeOption);

        // 历史数据趋势图
        this.charts.history = echarts.init(document.getElementById('historyChart'));
        const historyOption = {
            title: {
                text: '24小时数据趋势',
                textStyle: { fontSize: 14 }
            },
            tooltip: {
                trigger: 'axis'
            },
            legend: {
                data: ['温度', '湿度', '压力'],
                bottom: 0
            },
            xAxis: {
                type: 'category',
                data: this.generateHourLabels(24),
                axisLabel: { fontSize: 10 }
            },
            yAxis: {
                type: 'value',
                axisLabel: { fontSize: 10 }
            },
            series: [
                {
                    name: '温度',
                    type: 'line',
                    data: this.generateRandomData(24, 20, 30),
                    smooth: true
                },
                {
                    name: '湿度',
                    type: 'line',
                    data: this.generateRandomData(24, 40, 60),
                    smooth: true
                },
                {
                    name: '压力',
                    type: 'line',
                    data: this.generateRandomData(24, 1000, 1015),
                    smooth: true
                }
            ]
        };
        this.charts.history.setOption(historyOption);

        // 异常检测图
        this.charts.anomaly = echarts.init(document.getElementById('anomalyChart'));
        const anomalyOption = {
            title: {
                text: '异常检测',
                textStyle: { fontSize: 14 }
            },
            tooltip: {
                trigger: 'axis'
            },
            xAxis: {
                type: 'category',
                data: this.generateHourLabels(24),
                axisLabel: { fontSize: 10 }
            },
            yAxis: {
                type: 'value',
                axisLabel: { fontSize: 10 }
            },
            series: [{
                name: '异常数量',
                type: 'bar',
                data: this.generateRandomData(24, 0, 5),
                itemStyle: {
                    color: function(params) {
                        return params.value > 3 ? '#dc3545' : '#28a745';
                    }
                }
            }]
        };
        this.charts.anomaly.setOption(anomalyOption);

        // 资源使用图
        this.charts.resource = echarts.init(document.getElementById('resourceChart'));
        const resourceOption = {
            title: {
                text: '系统资源使用',
                textStyle: { fontSize: 14 }
            },
            tooltip: {
                trigger: 'axis'
            },
            legend: {
                data: ['CPU', '内存', '磁盘'],
                bottom: 0
            },
            xAxis: {
                type: 'category',
                data: this.generateTimeLabels(10),
                axisLabel: { fontSize: 10 }
            },
            yAxis: {
                type: 'value',
                max: 100,
                axisLabel: { fontSize: 10, formatter: '{value}%' }
            },
            series: [
                {
                    name: 'CPU',
                    type: 'line',
                    data: this.generateRandomData(10, 30, 70),
                    smooth: true
                },
                {
                    name: '内存',
                    type: 'line',
                    data: this.generateRandomData(10, 40, 80),
                    smooth: true
                },
                {
                    name: '磁盘',
                    type: 'line',
                    data: this.generateRandomData(10, 50, 90),
                    smooth: true
                }
            ]
        };
        this.charts.resource.setOption(resourceOption);

        // 响应式图表
        window.addEventListener('resize', () => {
            Object.values(this.charts).forEach(chart => chart.resize());
        });
    }

    // 初始化WebSocket连接
    initWebSocket() {
        const wsUrl = `ws://${window.location.host}/api/websocket`;
        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
            console.log('WebSocket connected');
            this.updateRobotStatus('WebSocket连接已建立');
        };

        this.ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.handleWebSocketMessage(data);
        };

        this.ws.onclose = () => {
            console.log('WebSocket disconnected');
            this.updateRobotStatus('WebSocket连接已断开');
            // 5秒后重连
            setTimeout(() => this.initWebSocket(), 5000);
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
            this.updateRobotStatus('WebSocket连接错误');
        };
    }

    // 处理WebSocket消息
    handleWebSocketMessage(data) {
        if (data.type === 'sensorData') {
            this.updateSensorDataTable(data.payload);
            this.updateMetrics(data.payload);
        } else if (data.type === 'robotStatus') {
            this.updateRobotStatus(data.payload);
        }
    }

    // 更新传感器数据表格
    updateSensorDataTable(data) {
        const tbody = document.getElementById('sensorDataTable');
        const row = document.createElement('tr');

        // 只保留最新的10条记录
        while (tbody.rows.length >= 10) {
            tbody.deleteRow(0);
        }

        row.innerHTML = `
            <td>${new Date(data.timestamp).toLocaleTimeString()}</td>
            <td>${data.robotId}</td>
            <td>${data.sensorType}</td>
            <td>${data.value.toFixed(2)} ${data.unit}</td>
            <td><span class="badge bg-success">正常</span></td>
        `;

        tbody.appendChild(row);
    }

    // 更新指标
    updateMetrics(data) {
        // 更新实时图表
        const realtimeChart = this.charts.realtime;
        const option = realtimeChart.getOption();

        // 添加新数据点
        option.series[0].data.push(Math.random() * 1000 + 500);
        if (option.series[0].data.length > 20) {
            option.series[0].data.shift();
        }

        realtimeChart.setOption(option);

        // 更新总数据点数
        const totalElement = document.getElementById('totalDataPoints');
        const currentTotal = parseInt(totalElement.textContent.replace(/,/g, ''));
        totalElement.textContent = (currentTotal + 1).toLocaleString();
    }

    // 加载机器人列表
    async loadRobots() {
        try {
            const response = await fetch(`${this.apiBase}/robot/list`);
            const result = await response.json();

            if (result.success) {
                this.updateRobotList(result.data);
                document.getElementById('activeRobots').textContent = result.data.length;
            }
        } catch (error) {
            console.error('Failed to load robots:', error);
        }
    }

    // 更新机器人列表
    updateRobotList(robots) {
        const listElement = document.getElementById('robotList');
        listElement.innerHTML = '';

        robots.forEach(robot => {
            const item = document.createElement('a');
            item.href = '#';
            item.className = 'list-group-item list-group-item-action';
            item.onclick = () => this.selectRobot(robot.id, robot.name);

            item.innerHTML = `
                <div class="d-flex w-100 justify-content-between">
                    <h6 class="mb-1">${robot.name}</h6>
                    <span class="status-indicator ${robot.online ? 'status-online' : 'status-offline'}"></span>
                </div>
                <p class="mb-1">ID: ${robot.id}</p>
            `;

            listElement.appendChild(item);
        });
    }

    // 选择机器人
    selectRobot(id, name) {
        this.selectedRobotId = id;
        document.getElementById('selectedRobot').textContent = name;

        // 高亮选中的机器人
        document.querySelectorAll('#robotList .list-group-item').forEach(item => {
            item.classList.remove('active');
        });
        event.target.closest('.list-group-item').classList.add('active');
    }

    // 发送控制命令
    async sendCommand(command) {
        if (!this.selectedRobotId) {
            alert('请先选择一个机器人');
            return;
        }

        try {
            const response = await fetch(`${this.apiBase}/robot/control/${this.selectedRobotId}/command`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ command })
            });

            const result = await response.json();
            if (result.success) {
                this.updateRobotStatus(`命令已发送: ${command}`);
            } else {
                this.updateRobotStatus(`命令发送失败: ${result.message}`);
            }
        } catch (error) {
            console.error('Failed to send command:', error);
            this.updateRobotStatus('命令发送失败');
        }
    }

    // 更新机器人状态
    updateRobotStatus(message) {
        const statusElement = document.getElementById('robotStatus');
        statusElement.textContent = message;
        statusElement.className = 'alert alert-info';
    }

    // 开始数据更新
    startDataUpdate() {
        // 每秒更新一次时间轴
        setInterval(() => {
            this.updateTimeAxes();
        }, 1000);

        // 每5秒更新一次指标
        setInterval(() => {
            this.updateSystemMetrics();
        }, 5000);
    }

    // 更新时间轴
    updateTimeAxes() {
        const now = new Date();

        // 更新实时图表
        const realtimeOption = this.charts.realtime.getOption();
        realtimeOption.xAxis[0].data.shift();
        realtimeOption.xAxis[0].data.push(now.toLocaleTimeString());
        this.charts.realtime.setOption(realtimeOption);

        // 更新资源图表
        const resourceOption = this.charts.resource.getOption();
        resourceOption.xAxis[0].data.shift();
        resourceOption.xAxis[0].data.push(now.toLocaleTimeString());
        this.charts.resource.setOption(resourceOption);
    }

    // 更新系统指标
    updateSystemMetrics() {
        // 模拟更新响应时间
        const avgResponseTime = Math.floor(Math.random() * 50 + 30);
        document.getElementById('avgResponseTime').textContent = avgResponseTime + 'ms';

        // 模拟更新系统健康度
        const systemHealth = (95 + Math.random() * 5).toFixed(1);
        document.getElementById('systemHealth').textContent = systemHealth + '%';
    }

    // 工具函数
    generateTimeLabels(count) {
        const labels = [];
        const now = new Date();
        for (let i = count - 1; i >= 0; i--) {
            const time = new Date(now - i * 1000);
            labels.push(time.toLocaleTimeString());
        }
        return labels;
    }

    generateHourLabels(hours) {
        const labels = [];
        const now = new Date();
        for (let i = hours - 1; i >= 0; i--) {
            const time = new Date(now - i * 60 * 60 * 1000);
            labels.push(time.getHours() + ':00');
        }
        return labels;
    }

    generateRandomData(count, min, max) {
        const data = [];
        for (let i = 0; i < count; i++) {
            data.push(Math.floor(Math.random() * (max - min + 1)) + min);
        }
        return data;
    }
}

// 页面切换函数
function showDashboard() {
    document.querySelectorAll('.nav-link').forEach(link => link.classList.remove('active'));
    event.target.classList.add('active');

    document.querySelectorAll('[id$="Content"]').forEach(content => {
        content.style.display = 'none';
    });
    document.getElementById('dashboardContent').style.display = 'block';
    document.getElementById('pageTitle').textContent = '实时监控面板';
}

function showDataAnalysis() {
    document.querySelectorAll('.nav-link').forEach(link => link.classList.remove('active'));
    event.target.classList.add('active');

    document.querySelectorAll('[id$="Content"]').forEach(content => {
        content.style.display = 'none';
    });
    document.getElementById('dataAnalysisContent').style.display = 'block';
    document.getElementById('pageTitle').textContent = '数据分析';
}

function showRobotControl() {
    document.querySelectorAll('.nav-link').forEach(link => link.classList.remove('active'));
    event.target.classList.add('active');

    document.querySelectorAll('[id$="Content"]').forEach(content => {
        content.style.display = 'none';
    });
    document.getElementById('robotControlContent').style.display = 'block';
    document.getElementById('pageTitle').textContent = '机器人控制';
}

function showSystemStatus() {
    document.querySelectorAll('.nav-link').forEach(link => link.classList.remove('active'));
    event.target.classList.add('active');

    document.querySelectorAll('[id$="Content"]').forEach(content => {
        content.style.display = 'none';
    });
    document.getElementById('systemStatusContent').style.display = 'block';
    document.getElementById('pageTitle').textContent = '系统状态';
}

function showSettings() {
    document.querySelectorAll('.nav-link').forEach(link => link.classList.remove('active'));
    event.target.classList.add('active');

    document.querySelectorAll('[id$="Content"]').forEach(content => {
        content.style.display = 'none';
    });
    document.getElementById('settingsContent').style.display = 'block';
    document.getElementById('pageTitle').textContent = '系统设置';
}

// 机器人控制函数
function sendCommand(command) {
    if (window.app) {
        window.app.sendCommand(command);
    }
}

// 页面加载完成后初始化应用
document.addEventListener('DOMContentLoaded', () => {
    window.app = new BDIRDPApp();
});