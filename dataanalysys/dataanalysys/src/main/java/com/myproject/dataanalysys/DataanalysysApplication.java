package com.myproject.dataanalysys;

import java.io.File;
import java.io.FileNotFoundException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.util.ResourceUtils;

@SpringBootApplication
@EnableAutoConfiguration
public class DataanalysysApplication {

	public static void main(String[] args) {
		SpringApplication.run(DataanalysysApplication.class, args);
	}
}
