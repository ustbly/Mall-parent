package com.sdust.mall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * 获取Jedis客户端的工具类
 */
object MyRedisUtil {
  //定义一个连接池对象
  private var jedisPool:JedisPool = null;

  //创建JedisPool连接池对象
  def build() = {

    val prop = MyPropertiesUtil.load("config.properties")
    val host = prop.getProperty("redis.host")
    val port = prop.getProperty("redis.port")

    val jedisPoolConfig:JedisPoolConfig = new JedisPoolConfig

    jedisPoolConfig.setMaxTotal(600)   //最大连接数
    jedisPoolConfig.setMaxIdle(20)     //最大空闲
    jedisPoolConfig.setMinIdle(20)     //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(500000)       //忙碌时等待时长（ms）
    jedisPoolConfig.setTestOnBorrow(true)        //每次获得连接的进行测试

    jedisPool = new JedisPool(jedisPoolConfig,host,port.toInt)

  }

  //获取Jedis客户端
  def getJedisClient():Jedis = {
    if(jedisPool == null) {
      build()
    }

    jedisPool.getResource
  }

  def main(args: Array[String]): Unit = {
    val jedis: Jedis = getJedisClient()
    println(jedis.ping())
    jedis.close()
  }
}