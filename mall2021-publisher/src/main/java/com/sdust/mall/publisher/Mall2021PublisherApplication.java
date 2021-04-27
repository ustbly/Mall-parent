package com.sdust.mall.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.sdust.mall.publisher.mapper")
public class Mall2021PublisherApplication {

	public static void main(String[] args) {
		SpringApplication.run(Mall2021PublisherApplication.class, args);
	}

}
