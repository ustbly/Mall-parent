package com.sdust.mall.realtime.util

import java.io.{FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * 读取配置文件的工具类
 */
object MyPropertiesUtil {
  def load(propertiesName:String) : Properties = {
    val prop: Properties = new Properties()

    //加载指定的配置文件
    prop.load(new InputStreamReader(
      Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
      StandardCharsets.UTF_8
    ))
    prop
  }

  def main(args: Array[String]): Unit = {
    val properties: Properties = MyPropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }
}
