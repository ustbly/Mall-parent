package com.sdust.mall.realtime.dim

import com.alibaba.fastjson.JSON
import com.sdust.mall.realtime.bean.BaseCategory3
import com.sdust.mall.realtime.util.MyKafkaUtil
import com.sdust.mall.realtime.util.OffsetManagerUitl
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 读取商品分类维度数据到 Hbase
 */
object BaseCategory3App {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new
        SparkConf().setMaster("local[4]").setAppName("BaseCategory3App")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_base_category3";
    val groupId = "dim_base_category3_group"
    ///////////////////// 偏移量处理///////////////////////////
    val offset: Map[TopicPartition, Long] = OffsetManagerUitl.getOffset(topic,
      groupId)
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从 redis 中读取当前最新偏移量 则用该偏移量加载 kafka 中的数据 否则直接用 kafka 读出默认最新的数据
    if (offset != null && offset.size > 0) {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
    } else {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDStream: DStream[ConsumerRecord[String, String]] =
      inputDStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    //转换结构
    val objectDStream: DStream[BaseCategory3] = inputGetOffsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val obj: BaseCategory3 = JSON.parseObject(jsonStr, classOf[BaseCategory3])
        obj
      }
    }
    //保存到 Hbase
    import org.apache.phoenix.spark._
    objectDStream.foreachRDD {
      rdd => {
        rdd.saveToPhoenix(
          "MALL2021_BASE_CATEGORY3",
          Seq("ID", "NAME", "CATEGORY2_ID"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181"))
        OffsetManagerUitl.saveOffset(topic, groupId, offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
