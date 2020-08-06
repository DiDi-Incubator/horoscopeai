/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: gaoyuxin@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.console

import java.text.SimpleDateFormat

import com.didichuxing.horoscope.util.Logging
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.{Config, ConfigFactory}
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.{Bean, ComponentScan}
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder
import org.springframework.scheduling.annotation.EnableScheduling
import springfox.documentation.builders.{PathSelectors, RequestHandlerSelectors}
import springfox.documentation.service.{ApiInfo, VendorExtension}
import springfox.documentation.spi.DocumentationType
import springfox.documentation.spring.web.plugins.Docket
import springfox.documentation.swagger2.annotations.EnableSwagger2

import scala.collection.JavaConverters._

@SpringBootApplication
@EnableSwagger2
@EnableScheduling
@ComponentScan(value = Array("com.didichuxing.horoscope.console"))
class HoroscopeConsoleMain extends Logging {

  @Bean
  def jacksonBuilder(): Jackson2ObjectMapperBuilder = {
    // used to support scala data types like Option and case class in spring-mvc
    new Jackson2ObjectMapperBuilder()
      .indentOutput(true)
      .dateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")) // javascript ISO 8601
      .modules(DefaultScalaModule)
  }

  @Bean
  def swaggerBuilder(): Docket = {
    new Docket(DocumentationType.SWAGGER_2)
      .select()
      .apis(RequestHandlerSelectors.any())
      .paths(PathSelectors.any())
      .build()
      .apiInfo(apiInfo)
  }

  val apiInfo = new ApiInfo(
    "Horoscope Console API",
    "Api Documentation", "1.0", "urn:tos", ApiInfo.DEFAULT_CONTACT,
    "Copyright (C) 2020 DiDi Inc. All Rights Reserved",
    "Copyright (C) 2020 DiDi Inc. All Rights Reserved",
    List.empty[VendorExtension[_]].asJava
  )

  @Bean
  implicit def config: Config =
    ConfigFactory.load(Thread.currentThread().getContextClassLoader, "application")
}

object HoroscopeConsoleMain extends Logging {
  def main(args: Array[String]): Unit = {
    info(s"Start application with config: ${args.mkString("[", ", ", "]")}")
    val ctx = SpringApplication.run(classOf[HoroscopeConsoleMain], args: _*)
    info("Let's inspect the beans provided by Spring Boot:")
    for (beanName <- ctx.getBeanDefinitionNames.sorted) {
      info(beanName)
    }
  }
}
