package com.sdust.mall.realtime.dim

import com.alibaba.fastjson.JSON
import com.sdust.mall.realtime.bean.ProvinceInfo
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
 * 从Kafka中读取省份数据，保存到Phoenix(Hbase)中
 */

object ProvinceInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("ProvinceInfoApp").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    var topic = "ods_base_province"
    var groupId = "province_info_group"

    //================1.从Kafka中读取数据=====================
    //1.1获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUitl.getOffset(topic, groupId)
    //1.2根据偏移量获取数据
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //1.3获取当前批次获取的偏移量的情况
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //================2.保存数据到Phoenix=====================
    import org.apache.phoenix.spark._
    offsetDStream.foreachRDD{
      rdd => {
        val provinceInfoRDD: RDD[ProvinceInfo] = rdd.map {
          record => {
            //获取省份的json字符串格式
            val jsonStr: String = record.value()
            //将json格式的字符串封装为ProvinceInfo样例类对象
            val proviceInfo: ProvinceInfo = JSON.parseObject(jsonStr, classOf[ProvinceInfo])
            proviceInfo
          }
        }

        provinceInfoRDD.saveToPhoenix(
          "MALL2021_PROVINCE_INFO",
          Seq("ID","NAME","AREA_CODE","ISO_CODE"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181")
        )

        //保存偏移量
        OffsetManagerUitl.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
