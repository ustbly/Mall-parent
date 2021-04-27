package com.sdust.mall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.sdust.mall.realtime.bean.DauInfo
import com.sdust.mall.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUitl}
import org.apache.commons.lang3.time.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.{ArrayStack, ListBuffer}

/**
 * 日活业务
 */
object DauApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("DauApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    //===============消费 Kafka 数据基本实现===================
    /*
    val date: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val dateArr: Array[String] = date.split("-")
    val month: String = dateArr(1)
    val day: String = dateArr(2)
    var topic: String = "mall_start_" + month + day
    var groupId: String = "mall_dau_" + month + day
     */
    var topic: String = "mall_start_0319"
    var groupId: String = "mall_dau_0319"

    //从Redis中获取Kafka分区偏移量
    val offSetMap: Map[TopicPartition, Long] = OffsetManagerUitl.getOffset(topic, groupId)

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null

    if(offSetMap != null || offSetMap.size > 0) {
      //如果Redis中存在当前消费者组对该主题的偏移量信息，那么从执行的偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offSetMap, groupId)
    }else {
      //如果Redis中不存在当前消费者组对该主题的偏移量信息，那么还是按照配置，从最新位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获取当前采集周期从Kafka中消费的数据的起始偏移量以及结束偏移量

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        //因为recordDStream底层封装的是KafkaRDD，混入了HasOffsetRanges特质，这个特质提供了可以获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //通过SparkStreaming从kafka中读取数据

    //val jsonDStream: DStream[String] = kafkaDStream.map(_.value())
    //测试输出 1
    //jsonDStream.print(1000)
    /**
     * {
     * "common":{
     * "ar":"440000",
     * "ba":"Xiaomi",
     * "ch":"web",
     * "md":"Xiaomi 10 Pro ",
     * "mid":"mid_1",
     * "os":"Android 8.1",
     * "uid":"373",
     * "vc":"v2.1.134"
     * },
     * "start":{
     * "entry":"icon",
     * "loading_time":3520,
     * "open_ad_id":2,
     * "open_ad_ms":1405,
     * "open_ad_skip_ms":0
     * },
     * "ts":1616488425000
     * }
     */

    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        //获取启动日志
        val jsonStr: String = record.value()
        //将json格式的字符串转为json对象
        val jSONObject: JSONObject = JSON.parseObject(jsonStr)
        //从json对象中获得时间戳
        val ts: lang.Long = jSONObject.getLong("ts")
        //将时间戳转为日期和小时
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        //对字符串日期和小时进行分割，分割后放到 json 对象中，方便后续处理
        val dateStrArr: Array[String] = dateStr.split(" ")

        val dt: String = dateStrArr(0)
        val hr: String = dateStrArr(1)
        jSONObject.put("dt", dt)
        jSONObject.put("hr", hr)
        jSONObject
      }
    }
    //测试输出 2
    //jsonObjDStream.print(1000)

    /*
    //通过Redis 对采集到的启动日志进行去重操作 方案1 采集周期中的每条数据都要获取一次redis连接，连接过于频繁
    //redis 类型：Set    key：dau:2021-03-23  value：mid   expire： 3600*24（一天）
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.filter {
      jsonObj => {

        //获取登录日期
        val dt: String = jsonObj.getString("dt")
        //获取设备id
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        //拼接Redis中保存登录信息的key
        var dauKey = "dau:" + dt
        //获取Jedis客户端
        val jedis: Jedis = MyRedisUtil.getJedisClient()

        //从redis中判断当前设备是否已登录过
        val isFirst: lang.Long = jedis.sadd(dauKey, mid)
        //设置key的失效时间
        if (jedis.ttl(dauKey) < 0) {
          jedis.expire(dauKey, 3600 * 24)
        }
        //关闭连接
        jedis.close()
        if (isFirst == 1L) {
          //说明是第一次登录
          true
        } else {
          //说明今天已经登录过了
          false
        }
      }
    }
    */

    //通过Redis 对采集到的启动日志进行去重操作 方案2 以分区为单位对数据进行处理，每一个分区获取一次redis连接
    //redis 类型：Set    key：dau:2021-03-23  value：mid   expire： 3600*24（一天）

    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
      jsonObjItr => { //以分区为单位对数据进行处理
        //每一个分区获取一次redis连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        //定义一个集合，用于存放当前分区中第一次登陆的日志
        val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
        //对分区的数据进行遍历
        for (jsonObj <- jsonObjItr) {
          //根据json对象获取日期属性
          val dt: String = jsonObj.getString("dt")
          //根据json对象获取设备id
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
          //拼接Redis中保存登录信息的key
          var dauKey = "dau:" + dt
          val isFirst: lang.Long = jedis.sadd(dauKey, mid)
          //设置key的失效时间
          if (jedis.ttl(dauKey) < 0) {
            jedis.expire(dauKey, 3600 * 24)
          }
          if (isFirst == 1L) {
            //说明是第一次登录
            filteredList.append(jsonObj)
          }
        }
        //关闭连接
        jedis.close()
        filteredList.toIterator
      }
    }

    //filteredDStream.count().print()

    //将数据批量地保存到ES中
    filteredDStream.foreachRDD {
      rdd => {
        //以分区为单位对数据进行处理
        rdd.foreachPartition {
          jsonObjItr => {
            val dauInfoList: List[(String,DauInfo)] = jsonObjItr.map {
              jsonObj => {
                //每次处理的是一个 json 对象 将 json 对象封装为样例类
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo: DauInfo = DauInfo(
                  commonJsonObj.getString("mid"), //设备 id
                  commonJsonObj.getString("uid"), //用户id
                  commonJsonObj.getString("ar"), //地区
                  commonJsonObj.getString("ch"), //渠道
                  commonJsonObj.getString("vc"), //版本
                  jsonObj.getString("dt"), //日期
                  jsonObj.getString("hr"), //小时
                  "00", //分钟我们前面没有转换，默认 00
                  jsonObj.getLong("ts") //时间戳
                )
                (dauInfo.mid,dauInfo)
              }
            }.toList

            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            //将数据批量保存到ES中
            MyESUtil.bulkInsert(dauInfoList,"mall2021_dau_info_" + dt)
          }
        }

        //提交偏移量到Redis中
        OffsetManagerUitl.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
  /*
  private def getYd(td: String): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var yd: String = null
    try {
      val tdDate: Date = dateFormat.parse(td)
      val ydDate: Date = DateUtils.addDays(tdDate, -1)
      yd = dateFormat.format(ydDate)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new RuntimeException("日期格式转变失败")
    }
    yd
  }
  */
}
