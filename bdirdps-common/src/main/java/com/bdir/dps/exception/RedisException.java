package com.bdir.dps.exception;

/**
 * Redis操作异常类
 * 用于处理Redis缓存操作相关异常
 */
public class RedisException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * 错误码
     */
    private String errorCode;

    /**
     * Redis键
     */
    private String key;

    public RedisException(String message) {
        super(message);
    }

    public RedisException(String message, Throwable cause) {
        super(message, cause);
    }

    public RedisException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public RedisException(String errorCode, String key, String message) {
        super(message);
        this.errorCode = errorCode;
        this.key = key;
    }

    public RedisException(String errorCode, String key, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.key = key;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}