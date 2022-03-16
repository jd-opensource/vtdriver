package com.jd.vtdriver.spring.boot.demo;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@MapperScan("com.jd.vtdriver.spring.boot.demo.mapper")
@SpringBootApplication
public class VtdriverSpringbootApplication {

    public static void main(String[] args) {
        SpringApplication.run(VtdriverSpringbootApplication.class, args);
    }

}
