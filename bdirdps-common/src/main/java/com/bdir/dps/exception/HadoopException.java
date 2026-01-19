package com.bdir.dps.exception;

/**
 * Hadoop操作异常类
 * 用于处理Hadoop HDFS、MapReduce等相关异常
 */
public class HadoopException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * 错误码
     */
    private String errorCode;

    /**
     * HDFS路径
     */
    private String hdfsPath;

    public HadoopException(String message) {
        super(message);
    }

    public HadoopException(String message, Throwable cause) {
        super(message, cause);
    }

    public HadoopException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public HadoopException(String errorCode, String hdfsPath, String message) {
        super(message);
        this.errorCode = errorCode;
        this.hdfsPath = hdfsPath;
    }

    public HadoopException(String errorCode, String hdfsPath, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.hdfsPath = hdfsPath;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
    }
}