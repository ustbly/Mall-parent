package com.sdust.mall.realtime.dim

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.sdust.mall.realtime.bean.UserInfo
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.sdust.mall.realtime.util.{MyKafkaUtil, OffsetManagerUitl}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * 从Kafka中读取用户数据，保存到Phoenix(Hbase)中
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.1基本环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("UserInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //1.2获取偏移量
    val topic = "ods_user_info"
    val groupId = "user_info_group"

    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUitl.getOffset(topic, groupId)
    //1.3获取本批次消费数据的偏移量情况

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null

    if (offsetMap != null && offsetMap.size > 0) {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //1.4对从Kafka中读取的数据进行结构的转换 record(k,v) ==> UserInfo
    offsetDStream.foreachRDD {
      rdd => {
        val userInfoRDD: RDD[UserInfo] = rdd.map {
          record => {
            val jsonStr: String = record.value()
            val userInfo: UserInfo = JSON.parseObject(jsonStr, classOf[UserInfo])
            //把生日转为年龄
            val format = new SimpleDateFormat("yyyy-MM-dd")
            val date: Date = format.parse(userInfo.birthday)
            val curTs: Long = System.currentTimeMillis()
            val betweenMs: Long = curTs - date.getTime
            val age: Long = betweenMs / 1000L / 60L / 60L / 24L / 365L
            if (age < 20) {
              userInfo.age_group = "20岁及以下"
            } else if (age > 30) {
              userInfo.age_group = "30岁以上"
            } else {
              userInfo.age_group = "21岁到30岁"
            }

            if (userInfo.gender == "M") {
              userInfo.gender_name = "男"
            } else {
              userInfo.gender_name = "女"
            }
            userInfo
          }
        }
        //1.5保存到Phoenix中
        import org.apache.phoenix.spark._
        userInfoRDD.saveToPhoenix(
          "MALL2021_USER_INFO",
          Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181")
        )
        //1.6保存偏移量
        OffsetManagerUitl.saveOffset(topic,groupId, offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
