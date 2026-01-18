package com.bdir.dps.security;

import com.bdir.dps.common.Result;
import com.bdir.dps.utils.JsonUtil;
import org.springframework.http.MediaType;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.stereotype.Component;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * JWT认证入口点
 * 处理未认证用户的访问请求
 *
 * @author BDIRDPSys开发团队
 * @since 2026-01-16
 */
@Component
public class JwtAuthenticationEntryPoint implements AuthenticationEntryPoint {

    /**
     * 处理认证异常
     * 返回JSON格式的错误信息
     */
    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response,
                         AuthenticationException authException) throws IOException, ServletException {

        // 设置响应状态码和类型
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding("UTF-8");

        // 构建错误响应
        Result<Void> result = Result.error("认证失败，请提供有效的访问令牌");

        // 写入响应体
        response.getWriter().write(JsonUtil.toJson(result));
    }
}