package com.bdir.dps.exception;

/**
 * WebSocket操作异常类
 * 用于处理WebSocket连接、消息推送等相关异常
 */
public class WebSocketException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * 错误码
     */
    private String errorCode;

    /**
     * WebSocket会话ID
     */
    private String sessionId;

    public WebSocketException(String message) {
        super(message);
    }

    public WebSocketException(String message, Throwable cause) {
        super(message, cause);
    }

    public WebSocketException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public WebSocketException(String errorCode, String sessionId, String message) {
        super(message);
        this.errorCode = errorCode;
        this.sessionId = sessionId;
    }

    public WebSocketException(String errorCode, String sessionId, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.sessionId = sessionId;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}