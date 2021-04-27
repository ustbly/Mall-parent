package com.sdust.mall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis


/**
 * 维护偏移量的工具类
 */
object OffsetManagerUitl {

  /**
   * 从Redis中获取偏移量
   * 格式：type=>Hash
   * key   =>  offset:topic:groupId
   * field =>  partitionId
   * value =>  偏移量值
   * expire 不需要指定
   *
   * @param topic   主题名称
   * @param groupId 消费者组
   * @return 当前消费者组中，消费的主题对应的分区的偏移量信息
   *         KafkaUtils.createDirectStream 在读取数据的时候封装了Map[TopicPartition,Long]
   */
  def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    //获取客户端连接
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    //拼接操作redis的key       offset:topic:groupId
    var offsetKey = "offset:" + topic + ":" + groupId

    //获取当前消费者组消费的主题 对应的分区以及偏移量情况
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    //关闭连接
    jedis.close()
    //将 Java 的 Map 转换为 Scala 的 Map，方便后续操作
    import scala.collection.JavaConverters.mapAsScalaMapConverter
    val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => {
        //Map[TopicPartition, Long]
        println("读取分区偏移量：" + partition + ":" + offset)
        //将 Redis 中保存的分区对应的偏移量进行封装
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }.toMap
    kafkaOffsetMap
  }


  /**
   * 向Redis中保存偏移量
   * Redis 格式：
   * type   =>  Hash
   * key    =>  offset:topic:groupId
   * field  =>  partitionId
   * value  =>  偏移量值
   * expire =>  不需要指定
   *
   * @param topicName 主题名
   * @param groupId 消费者组
   * @param offsetRanges 当前消费者组中，消费的主题对应的分区的偏移量起始和结束信息
   */

  def saveOffset(topicName: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    //拼接操作redis的key       offset:topic:groupId
    var offsetKey = "offset:" + topicName + ":" + groupId
    //定义java的map集合，用于存放每个分区对应偏移量
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()
    //对offsetRanges进行遍历，将数据封装到offsetMap
    for (offsetRange <- offsetRanges) {
      val partitionId: Int = offsetRange.partition
      val fromOffset: Long = offsetRange.fromOffset
      val untilOffset: Long = offsetRange.untilOffset

      offsetMap.put(partitionId.toString,untilOffset.toString)
      println("保存分区：" + partitionId + ":" + fromOffset + "------>" + untilOffset)
    }
    jedis.hmset(offsetKey,offsetMap)

    jedis.close()
  }

}
