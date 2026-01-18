package com.bdir.dps.exception;

import com.bdir.dps.common.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.validation.BindException;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 全局异常处理器
 * 统一处理系统中的各种异常
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * 处理业务异常
     */
    @ExceptionHandler(BusinessException.class)
    public Result<Object> handleBusinessException(BusinessException e) {
        log.error("业务异常: {}", e.getMessage(), e);
        return Result.error(e.getCode(), e.getMessage());
    }

    /**
     * 处理参数校验异常
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public Result<Object> handleMethodArgumentNotValidException(MethodArgumentNotValidException e) {
        log.error("参数校验异常: {}", e.getMessage(), e);
        BindingResult bindingResult = e.getBindingResult();
        Map<String, String> errors = new HashMap<>();

        for (FieldError fieldError : bindingResult.getFieldErrors()) {
            errors.put(fieldError.getField(), fieldError.getDefaultMessage());
        }

        return Result.error(400, "参数校验失败", errors);
    }

    /**
     * 处理绑定异常
     */
    @ExceptionHandler(BindException.class)
    public Result<Object> handleBindException(BindException e) {
        log.error("绑定异常: {}", e.getMessage(), e);
        BindingResult bindingResult = e.getBindingResult();
        Map<String, String> errors = new HashMap<>();

        for (FieldError fieldError : bindingResult.getFieldErrors()) {
            errors.put(fieldError.getField(), fieldError.getDefaultMessage());
        }

        return Result.error(400, "参数绑定失败", errors);
    }

    /**
     * 处理约束违反异常
     */
    @ExceptionHandler(ConstraintViolationException.class)
    public Result<Object> handleConstraintViolationException(ConstraintViolationException e) {
        log.error("约束违反异常: {}", e.getMessage(), e);
        Set<ConstraintViolation<?>> violations = e.getConstraintViolations();
        String message = violations.stream()
                .map(ConstraintViolation::getMessage)
                .collect(Collectors.joining(", "));

        return Result.error(400, message);
    }

    /**
     * 处理参数类型不匹配异常
     */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public Result<Object> handleMethodArgumentTypeMismatchException(MethodArgumentTypeMismatchException e) {
        log.error("参数类型不匹配异常: {}", e.getMessage(), e);
        String message = String.format("参数[%s]类型不匹配，期望类型为%s",
                e.getName(), e.getRequiredType().getSimpleName());

        return Result.error(400, message);
    }

    /**
     * 处理404异常
     */
    @ExceptionHandler(NoHandlerFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Result<Object> handleNoHandlerFoundException(NoHandlerFoundException e) {
        log.error("404异常: {}", e.getMessage(), e);
        return Result.error(404, "请求的资源不存在");
    }

    /**
     * 处理数据库异常
     */
    @ExceptionHandler(SQLException.class)
    public Result<Object> handleSQLException(SQLException e) {
        log.error("数据库异常: {}", e.getMessage(), e);
        String message = "数据库操作失败";

        // 根据SQLState判断具体错误类型
        String sqlState = e.getSQLState();
        if (sqlState != null) {
            switch (sqlState) {
                case "23000":
                    message = "数据完整性约束违反";
                    break;
                case "42S02":
                    message = "表或视图不存在";
                    break;
                case "42S22":
                    message = "列不存在";
                    break;
                case "08001":
                    message = "数据库连接失败";
                    break;
                case "HY000":
                    message = "数据库内部错误";
                    break;
            }
        }

        return Result.error(500, message);
    }

    /**
     * 处理Hive异常
     */
    @ExceptionHandler(HiveException.class)
    public Result<Object> handleHiveException(HiveException e) {
        log.error("Hive异常: {}", e.getMessage(), e);
        return Result.error(500, "Hive查询失败: " + e.getMessage());
    }

    /**
     * 处理Kafka异常
     */
    @ExceptionHandler(KafkaException.class)
    public Result<Object> handleKafkaException(KafkaException e) {
        log.error("Kafka异常: {}", e.getMessage(), e);
        return Result.error(500, "消息队列操作失败: " + e.getMessage());
    }

    /**
     * 处理Redis异常
     */
    @ExceptionHandler(RedisException.class)
    public Result<Object> handleRedisException(RedisException e) {
        log.error("Redis异常: {}", e.getMessage(), e);
        return Result.error(500, "缓存操作失败: " + e.getMessage());
    }

    /**
     * 处理Hadoop异常
     */
    @ExceptionHandler(HadoopException.class)
    public Result<Object> handleHadoopException(HadoopException e) {
        log.error("Hadoop异常: {}", e.getMessage(), e);
        return Result.error(500, "分布式存储操作失败: " + e.getMessage());
    }

    /**
     * 处理WebSocket异常
     */
    @ExceptionHandler(WebSocketException.class)
    public Result<Object> handleWebSocketException(WebSocketException e) {
        log.error("WebSocket异常: {}", e.getMessage(), e);
        return Result.error(500, "实时通信失败: " + e.getMessage());
    }

    /**
     * 处理空指针异常
     */
    @ExceptionHandler(NullPointerException.class)
    public Result<Object> handleNullPointerException(NullPointerException e) {
        log.error("空指针异常: {}", e.getMessage(), e);
        return Result.error(500, "系统内部错误：空指针异常");
    }

    /**
     * 处理数组越界异常
     */
    @ExceptionHandler(IndexOutOfBoundsException.class)
    public Result<Object> handleIndexOutOfBoundsException(IndexOutOfBoundsException e) {
        log.error("数组越界异常: {}", e.getMessage(), e);
        return Result.error(500, "系统内部错误：数组越界");
    }

    /**
     * 处理类转换异常
     */
    @ExceptionHandler(ClassCastException.class)
    public Result<Object> handleClassCastException(ClassCastException e) {
        log.error("类转换异常: {}", e.getMessage(), e);
        return Result.error(500, "系统内部错误：类型转换失败");
    }

    /**
     * 处理非法参数异常
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public Result<Object> handleIllegalArgumentException(IllegalArgumentException e) {
        log.error("非法参数异常: {}", e.getMessage(), e);
        return Result.error(400, "参数错误：" + e.getMessage());
    }

    /**
     * 处理非法状态异常
     */
    @ExceptionHandler(IllegalStateException.class)
    public Result<Object> handleIllegalStateException(IllegalStateException e) {
        log.error("非法状态异常: {}", e.getMessage(), e);
        return Result.error(500, "系统状态异常：" + e.getMessage());
    }

    /**
     * 处理不支持的操作异常
     */
    @ExceptionHandler(UnsupportedOperationException.class)
    public Result<Object> handleUnsupportedOperationException(UnsupportedOperationException e) {
        log.error("不支持的操作异常: {}", e.getMessage(), e);
        return Result.error(405, "不支持的操作：" + e.getMessage());
    }

    /**
     * 处理系统异常
     */
    @ExceptionHandler(Exception.class)
    public Result<Object> handleException(Exception e) {
        log.error("系统异常: {}", e.getMessage(), e);
        return Result.error(500, "系统内部错误，请联系管理员");
    }

    /**
     * 处理Throwable
     */
    @ExceptionHandler(Throwable.class)
    public Result<Object> handleThrowable(Throwable e) {
        log.error("系统错误: {}", e.getMessage(), e);
        return Result.error(500, "系统发生严重错误");
    }
}

/**
 * 业务异常
 */
class BusinessException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final int code;

    public BusinessException(int code, String message) {
        super(message);
        this.code = code;
    }

    public BusinessException(String message) {
        this(400, message);
    }

    public int getCode() {
        return code;
    }
}

/**
 * Hive异常
 */
class HiveException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public HiveException(String message) {
        super(message);
    }

    public HiveException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * Kafka异常
 */
class KafkaException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public KafkaException(String message) {
        super(message);
    }

    public KafkaException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * Redis异常
 */
class RedisException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public RedisException(String message) {
        super(message);
    }

    public RedisException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * Hadoop异常
 */
class HadoopException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public HadoopException(String message) {
        super(message);
    }

    public HadoopException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * WebSocket异常
 */
class WebSocketException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public WebSocketException(String message) {
        super(message);
    }

    public WebSocketException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * 数据异常
 */
class DataException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public DataException(String message) {
        super(message);
    }

    public DataException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * 机器人异常
 */
class RobotException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public RobotException(String message) {
        super(message);
    }

    public RobotException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * 传感器异常
 */
class SensorException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public SensorException(String message) {
        super(message);
    }

    public SensorException(String message, Throwable cause) {
        super(message, cause);
    }
}

/**
 * 异常工具类
 */
class ExceptionUtils {

    /**
     * 抛出业务异常
     */
    public static void throwBusinessException(String message) {
        throw new BusinessException(message);
    }

    /**
     * 抛出业务异常
     */
    public static void throwBusinessException(int code, String message) {
        throw new BusinessException(code, message);
    }

    /**
     * 抛出Hive异常
     */
    public static void throwHiveException(String message) {
        throw new HiveException(message);
    }

    /**
     * 抛出Hive异常
     */
    public static void throwHiveException(String message, Throwable cause) {
        throw new HiveException(message, cause);
    }

    /**
     * 抛出Kafka异常
     */
    public static void throwKafkaException(String message) {
        throw new KafkaException(message);
    }

    /**
     * 抛出Kafka异常
     */
    public static void throwKafkaException(String message, Throwable cause) {
        throw new KafkaException(message, cause);
    }

    /**
     * 抛出Redis异常
     */
    public static void throwRedisException(String message) {
        throw new RedisException(message);
    }

    /**
     * 抛出Redis异常
     */
    public static void throwRedisException(String message, Throwable cause) {
        throw new RedisException(message, cause);
    }

    /**
     * 抛出Hadoop异常
     */
    public static void throwHadoopException(String message) {
        throw new HadoopException(message);
    }

    /**
     * 抛出Hadoop异常
     */
    public static void throwHadoopException(String message, Throwable cause) {
        throw new HadoopException(message, cause);
    }

    /**
     * 抛出WebSocket异常
     */
    public static void throwWebSocketException(String message) {
        throw new WebSocketException(message);
    }

    /**
     * 抛出WebSocket异常
     */
    public static void throwWebSocketException(String message, Throwable cause) {
        throw new WebSocketException(message, cause);
    }

    /**
     * 抛出数据异常
     */
    public static void throwDataException(String message) {
        throw new DataException(message);
    }

    /**
     * 抛出数据异常
     */
    public static void throwDataException(String message, Throwable cause) {
        throw new DataException(message, cause);
    }

    /**
     * 抛出机器人异常
     */
    public static void throwRobotException(String message) {
        throw new RobotException(message);
    }

    /**
     * 抛出机器人异常
     */
    public static void throwRobotException(String message, Throwable cause) {
        throw new RobotException(message, cause);
    }

    /**
     * 抛出传感器异常
     */
    public static void throwSensorException(String message) {
        throw new SensorException(message);
    }

    /**
     * 抛出传感器异常
     */
    public static void throwSensorException(String message, Throwable cause) {
        throw new SensorException(message, cause);
    }
}