package com.bdir.dps.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.io.Serializable;

/**
 * 统一响应结果封装类
 * @param <T> 数据类型
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "统一响应结果")
public class Result<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    @Schema(description = "响应码", required = true, example = "200")
    private int code;

    @Schema(description = "响应消息", required = true, example = "操作成功")
    private String message;

    @Schema(description = "响应数据")
    private T data;

    @Schema(description = "时间戳", required = true, example = "1640000000000")
    private long timestamp;

    @Schema(description = "请求ID", example = "req-123456")
    private String requestId;

    public Result() {
        this.timestamp = System.currentTimeMillis();
    }

    public Result(int code, String message) {
        this();
        this.code = code;
        this.message = message;
    }

    public Result(int code, String message, T data) {
        this(code, message);
        this.data = data;
    }

    /**
     * 成功响应
     */
    public static <T> Result<T> success() {
        return new Result<>(200, "操作成功");
    }

    /**
     * 成功响应，带数据
     */
    public static <T> Result<T> success(T data) {
        return new Result<>(200, "操作成功", data);
    }

    /**
     * 成功响应，自定义消息
     */
    public static <T> Result<T> success(String message) {
        return new Result<>(200, message);
    }

    /**
     * 成功响应，自定义消息和数据
     */
    public static <T> Result<T> success(String message, T data) {
        return new Result<>(200, message, data);
    }

    /**
     * 失败响应
     */
    public static <T> Result<T> error() {
        return new Result<>(500, "操作失败");
    }

    /**
     * 失败响应，自定义消息
     */
    public static <T> Result<T> error(String message) {
        return new Result<>(500, message);
    }

    /**
     * 失败响应，自定义状态码和消息
     */
    public static <T> Result<T> error(int code, String message) {
        return new Result<>(code, message);
    }

    /**
     * 失败响应，自定义状态码、消息和数据
     */
    public static <T> Result<T> error(int code, String message, T data) {
        return new Result<>(code, message, data);
    }

    /**
     * 参数错误
     */
    public static <T> Result<T> paramError(String message) {
        return new Result<>(400, message);
    }

    /**
     * 参数错误，带数据
     */
    public static <T> Result<T> paramError(String message, T data) {
        return new Result<>(400, message, data);
    }

    /**
     * 未授权
     */
    public static <T> Result<T> unauthorized(String message) {
        return new Result<>(401, message);
    }

    /**
     * 无权限
     */
    public static <T> Result<T> forbidden(String message) {
        return new Result<>(403, message);
    }

    /**
     * 资源不存在
     */
    public static <T> Result<T> notFound(String message) {
        return new Result<>(404, message);
    }

    /**
     * 系统错误
     */
    public static <T> Result<T> systemError(String message) {
        return new Result<>(500, message);
    }

    /**
     * 服务不可用
     */
    public static <T> Result<T> serviceUnavailable(String message) {
        return new Result<>(503, message);
    }

    /**
     * 判断响应是否成功
     */
    public boolean isSuccess() {
        return this.code == 200;
    }

    /**
     * 判断响应是否失败
     */
    public boolean isError() {
        return this.code != 200;
    }

    /**
     * 添加请求ID
     */
    public Result<T> withRequestId(String requestId) {
        this.requestId = requestId;
        return this;
    }

    /**
     * 添加数据
     */
    public Result<T> withData(T data) {
        this.data = data;
        return this;
    }

    /**
     * 构建分页结果
     */
    public static <T> Result<PageResult<T>> page(PageResult<T> pageData) {
        return success(pageData);
    }

    /**
     * 构建分页结果
     */
    public static <T> Result<PageResult<T>> page(long total, long page, long size, T records) {
        PageResult<T> pageResult = new PageResult<>(total, page, size, records);
        return success(pageResult);
    }
}

/**
 * 分页结果封装类
 */
@Data
@Schema(description = "分页结果")
class PageResult<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    @Schema(description = "总记录数", required = true, example = "100")
    private long total;

    @Schema(description = "当前页码", required = true, example = "1")
    private long page;

    @Schema(description = "每页记录数", required = true, example = "20")
    private long size;

    @Schema(description = "总页数", required = true, example = "5")
    private long pages;

    @Schema(description = "当前页数据", required = true)
    private T records;

    public PageResult() {
    }

    public PageResult(long total, long page, long size, T records) {
        this.total = total;
        this.page = page;
        this.size = size;
        this.records = records;
        this.pages = total % size == 0 ? total / size : total / size + 1;
    }

    /**
     * 获取偏移量
     */
    public long getOffset() {
        return (page - 1) * size;
    }

    /**
     * 是否有下一页
     */
    public boolean hasNext() {
        return page < pages;
    }

    /**
     * 是否有上一页
     */
    public boolean hasPrevious() {
        return page > 1;
    }

    /**
     * 获取下一页页码
     */
    public long nextPage() {
        return hasNext() ? page + 1 : page;
    }

    /**
     * 获取上一页页码
     */
    public long previousPage() {
        return hasPrevious() ? page - 1 : page;
    }
}

/**
 * 结果工具类
 */
class ResultUtils {

    /**
     * 成功结果
     */
    public static <T> Result<T> ok() {
        return Result.success();
    }

    /**
     * 成功结果，带数据
     */
    public static <T> Result<T> ok(T data) {
        return Result.success(data);
    }

    /**
     * 成功结果，自定义消息
     */
    public static <T> Result<T> ok(String message) {
        return Result.success(message);
    }

    /**
     * 成功结果，自定义消息和数据
     */
    public static <T> Result<T> ok(String message, T data) {
        return Result.success(message, data);
    }

    /**
     * 失败结果
     */
    public static <T> Result<T> fail() {
        return Result.error();
    }

    /**
     * 失败结果，自定义消息
     */
    public static <T> Result<T> fail(String message) {
        return Result.error(message);
    }

    /**
     * 失败结果，自定义状态码和消息
     */
    public static <T> Result<T> fail(int code, String message) {
        return Result.error(code, message);
    }

    /**
     * 参数错误
     */
    public static <T> Result<T> paramError(String message) {
        return Result.paramError(message);
    }

    /**
     * 未授权
     */
    public static <T> Result<T> unauthorized(String message) {
        return Result.unauthorized(message);
    }

    /**
     * 无权限
     */
    public static <T> Result<T> forbidden(String message) {
        return Result.forbidden(message);
    }

    /**
     * 资源不存在
     */
    public static <T> Result<T> notFound(String message) {
        return Result.notFound(message);
    }

    /**
     * 系统错误
     */
    public static <T> Result<T> systemError(String message) {
        return Result.systemError(message);
    }

    /**
     * 服务不可用
     */
    public static <T> Result<T> serviceUnavailable(String message) {
        return Result.serviceUnavailable(message);
    }

    /**
     * 分页结果
     */
    public static <T> Result<PageResult<T>> page(long total, long page, long size, T records) {
        return Result.page(total, page, size, records);
    }

    /**
     * 分页结果
     */
    public static <T> Result<PageResult<T>> page(PageResult<T> pageData) {
        return Result.page(pageData);
    }

    /**
     * 判断是否成功
     */
    public static boolean isSuccess(Result<?> result) {
        return result != null && result.isSuccess();
    }

    /**
     * 判断是否失败
     */
    public static boolean isError(Result<?> result) {
        return result == null || result.isError();
    }
}