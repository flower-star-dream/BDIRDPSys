package com.bdir.dps.controller;

import com.bdir.dps.common.Result;
import com.bdir.dps.dto.LoginRequest;
import com.bdir.dps.dto.LoginResponse;
import com.bdir.dps.dto.SignUpRequest;
import com.bdir.dps.entity.User;
import com.bdir.dps.service.AuthService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

/**
 * 认证控制器
 * 处理用户登录、注册等认证相关请求
 *
 * @author BDIRDPSys开发团队
 * @since 2026-01-16
 */
@RestController
@RequestMapping("/api/auth")
public class AuthController {

    @Autowired
    private AuthService authService;

    /**
     * 用户登录
     * 验证用户凭据并返回JWT令牌
     */
    @PostMapping("/login")
    public ResponseEntity<Result<LoginResponse>> authenticateUser(@Validated @RequestBody LoginRequest loginRequest) {
        try {
            LoginResponse loginResponse = authService.authenticateUser(loginRequest);
            return ResponseEntity.ok(Result.success(loginResponse));
        } catch (Exception e) {
            return ResponseEntity.ok(Result.error("登录失败：" + e.getMessage()));
        }
    }

    /**
     * 用户注册
     * 创建新用户账户
     */
    @PostMapping("/signup")
    public ResponseEntity<Result<User>> registerUser(@Validated @RequestBody SignUpRequest signUpRequest) {
        try {
            User user = authService.registerUser(signUpRequest);
            return ResponseEntity.ok(Result.success(user));
        } catch (Exception e) {
            return ResponseEntity.ok(Result.error("注册失败：" + e.getMessage()));
        }
    }

    /**
     * 刷新JWT令牌
     * 使用刷新令牌获取新的访问令牌
     */
    @PostMapping("/refresh")
    public ResponseEntity<Result<LoginResponse>> refreshToken(@RequestParam String refreshToken) {
        try {
            LoginResponse loginResponse = authService.refreshToken(refreshToken);
            return ResponseEntity.ok(Result.success(loginResponse));
        } catch (Exception e) {
            return ResponseEntity.ok(Result.error("刷新令牌失败：" + e.getMessage()));
        }
    }

    /**
     * 登出
     * 使JWT令牌失效
     */
    @PostMapping("/logout")
    public ResponseEntity<Result<Void>> logout(@RequestHeader("Authorization") String token) {
        try {
            authService.logout(token);
            return ResponseEntity.ok(Result.success(null));
        } catch (Exception e) {
            return ResponseEntity.ok(Result.error("登出失败：" + e.getMessage()));
        }
    }

    /**
     * 检查用户名是否可用
     */
    @GetMapping("/check-username")
    public ResponseEntity<Result<Boolean>> checkUsername(@RequestParam String username) {
        boolean available = authService.isUsernameAvailable(username);
        return ResponseEntity.ok(Result.success(available));
    }

    /**
     * 检查邮箱是否可用
     */
    @GetMapping("/check-email")
    public ResponseEntity<Result<Boolean>> checkEmail(@RequestParam String email) {
        boolean available = authService.isEmailAvailable(email);
        return ResponseEntity.ok(Result.success(available));
    }
}