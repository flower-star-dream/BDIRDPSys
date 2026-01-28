package com.bdir.dps.utils;

import io.jsonwebtoken.*;
import io.jsonwebtoken.security.Keys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

import java.security.Key;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Date;

/**
 * JWT令牌提供者
 * 负责生成和验证JWT令牌
 *
 * @author BDIRDPSys开发团队
 * @since 2026-01-16
 */
@Component
public class JwtTokenProvider {

    @Value("${app.jwt.secret}")
    private String jwtSecret;

    @Value("${app.jwt.expiration:86400000}")
    private int jwtExpirationInMs;

    /**
     * 生成安全的JWT密钥
     * 如果配置中没有提供密钥，则生成一个随机的256位密钥
     */
    private Key getSigningKey() {
        // 如果密钥为空或太短，生成一个安全的随机密钥
        if (jwtSecret == null || jwtSecret.length() < 32) {
            jwtSecret = generateSecureKey();
        }
        return Keys.hmacShaKeyFor(jwtSecret.getBytes());
    }

    /**
     * 生成安全的随机密钥
     */
    private String generateSecureKey() {
        SecureRandom random = new SecureRandom();
        byte[] keyBytes = new byte[32]; // 256位
        random.nextBytes(keyBytes);
        return Base64.getEncoder().encodeToString(keyBytes);
    }

    /**
     * 生成JWT令牌
     */
    public String generateToken(Authentication authentication) {
        UserDetails userPrincipal = (UserDetails) authentication.getPrincipal();
        Date expiryDate = new Date(System.currentTimeMillis() + jwtExpirationInMs);

        return Jwts.builder()
                .setSubject(userPrincipal.getUsername())
                .setIssuedAt(new Date())
                .setExpiration(expiryDate)
                .signWith(getSigningKey(), SignatureAlgorithm.HS256)
                .compact();
    }

    /**
     * 从令牌中获取用户名
     */
    public String getUsernameFromToken(String token) {
        Claims claims = Jwts.parserBuilder()
                .setSigningKey(getSigningKey())
                .build()
                .parseClaimsJws(token)
                .getBody();

        return claims.getSubject();
    }

    /**
     * 验证JWT令牌
     */
    public boolean validateToken(String authToken) {
        try {
            Jwts.parserBuilder()
                    .setSigningKey(getSigningKey())
                    .build()
                    .parseClaimsJws(authToken);
            return true;
        } catch (SecurityException ex) {
            System.err.println("无效的JWT签名");
        } catch (MalformedJwtException ex) {
            System.err.println("无效的JWT令牌");
        } catch (ExpiredJwtException ex) {
            System.err.println("JWT令牌已过期");
        } catch (UnsupportedJwtException ex) {
            System.err.println("不支持的JWT令牌");
        } catch (IllegalArgumentException ex) {
            System.err.println("JWT声明为空");
        }
        return false;
    }

    /**
     * 从令牌中获取过期时间
     */
    public Date getExpirationDateFromToken(String token) {
        Claims claims = Jwts.parserBuilder()
                .setSigningKey(getSigningKey())
                .build()
                .parseClaimsJws(token)
                .getBody();

        return claims.getExpiration();
    }

    /**
     * 检查令牌是否已过期
     */
    public Boolean isTokenExpired(String token) {
        final Date expiration = getExpirationDateFromToken(token);
        return expiration.before(new Date());
    }
}