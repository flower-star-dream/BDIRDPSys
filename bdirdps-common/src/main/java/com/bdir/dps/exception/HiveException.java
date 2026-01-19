package com.bdir.dps.exception;

/**
 * Hive操作异常类
 * 用于处理Hive查询、表操作等相关异常
 */
public class HiveException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * 错误码
     */
    private String errorCode;

    public HiveException(String message) {
        super(message);
    }

    public HiveException(String message, Throwable cause) {
        super(message, cause);
    }

    public HiveException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public HiveException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }
}