package com.bdir.dps;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * BDIRDPSys主启动类
 */
@SpringBootApplication
@EnableAsync
@EnableScheduling
@MapperScan("com.bdir.dps.mapper")
public class BdirDpsApplication {

    public static void main(String[] args) {
        SpringApplication.run(BdirDpsApplication.class, args);
    }
}