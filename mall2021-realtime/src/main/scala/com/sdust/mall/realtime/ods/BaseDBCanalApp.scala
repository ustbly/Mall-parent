package com.sdust.mall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.sdust.mall.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUitl}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * 从Kafka读取数据，根据表名进行分流处理（canal）
 */
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))


    var topic = "mall2021_db_c"
    var groupId = "base_db_canal_group"

    //从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUitl.getOffset(topic, groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size != 0) {
      //从指定的偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      //从最新位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获取当前批次读取的Kafka主题中偏移量信息
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //对接收到的数据进行数据结构的转换  ConsumerRecord[String, String(jsonStr)] -> jsonObj
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        //获取到json格式的字符串
        val jsonStr: String = record.value()
        //将json格式字符串转换为json对象
        val jSONObject: JSONObject = JSON.parseObject(jsonStr)
        jSONObject
      }
    }

    //分流：根据不同的表名，将数据发送到不同的kafka主题中
    jsonObjDStream.foreachRDD {
      rdd => {
        rdd.foreach {
          jsonObj => {
            //获取操作类型
            val opType: String = jsonObj.getString("type")
            if ("INSERT".equals(opType)) {
              //获取表名
              val tableName: String = jsonObj.getString("table")
              //获取操作数据
              val dataArr: JSONArray = jsonObj.getJSONArray("data")
              //拼接目标topic名称
              var sendTopic = "ods_" + tableName
              //对数组类型的操作数据进行遍历
              for (data <- dataArr.asScala) {
                //根据表名将数据发送到不同主题中去
                MyKafkaSink.send(sendTopic,data.toString)
              }
            }
          }
        }

        //提交偏移量
        OffsetManagerUitl.saveOffset(topic,groupId, offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
