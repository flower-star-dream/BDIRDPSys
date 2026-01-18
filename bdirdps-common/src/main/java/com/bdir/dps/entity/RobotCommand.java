package com.bdir.dps.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * 机器人控制指令实体类
 * 用于存储和传输机器人控制指令
 */
@Data
@Accessors(chain = true)
public class RobotCommand {

    /**
     * 指令ID
     */
    private String commandId;

    /**
     * 机器人ID
     */
    @NotBlank(message = "机器人ID不能为空")
    private String robotId;

    /**
     * 指令类型
     */
    @NotBlank(message = "指令类型不能为空")
    private String commandType;

    /**
     * 指令参数
     */
    private Map<String, Object> parameters;

    /**
     * 优先级
     */
    @NotNull(message = "优先级不能为空")
    private Integer priority = 5;

    /**
     * 指令状态
     */
    private String status = "PENDING";

    /**
     * 创建时间
     */
    private LocalDateTime timestamp;

    /**
     * 执行时间
     */
    private LocalDateTime executeTime;

    /**
     * 完成时间
     */
    private LocalDateTime completeTime;

    /**
     * 错误信息
     */
    private String errorMessage;

    /**
     * 执行结果
     */
    private String result;

    /**
     * 重试次数
     */
    private Integer retryCount = 0;

    /**
     * 最大重试次数
     */
    private Integer maxRetry = 3;

    /**
     * 指令超时时间（秒）
     */
    private Integer timeout = 30;
}