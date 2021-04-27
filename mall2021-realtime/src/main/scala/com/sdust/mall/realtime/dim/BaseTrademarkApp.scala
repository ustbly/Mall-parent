package com.sdust.mall.realtime.dim

import com.alibaba.fastjson.JSON
import com.sdust.mall.realtime.bean.BaseTrademark
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
 * 从Kafka中读取品牌维度数据，写入到Hbase中
 */
object BaseTrademarkApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("BaseTrademarkApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ods_base_trademark"
    val groupId = "dim_base_trademark_group"
    ///////////////////// 偏移量处理///////////////////////////
    val offset: Map[TopicPartition, Long] = OffsetManagerUitl.getOffset(topic, groupId)
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从 redis 中读取当前最新偏移量 则用该偏移量加载 kafka 中的数据 否则直接用kafka 读出默认最新的数据
    if (offset != null && offset.size > 0) {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
    } else {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDStream: DStream[ConsumerRecord[String, String]] =
      inputDStream.transform {
        rdd => {
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }
    val objectDStream: DStream[BaseTrademark] = inputGetOffsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val obj: BaseTrademark = JSON.parseObject(jsonStr, classOf[BaseTrademark])
        obj
      }
    }
    import org.apache.phoenix.spark._
    //保存到 Hbase
    objectDStream.foreachRDD {
      rdd => {
        rdd.saveToPhoenix(
          "MALL2021_BASE_TRADEMARK",
          Seq("ID", "TM_NAME"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181")
        )
        OffsetManagerUitl.saveOffset(topic, groupId, offsetRanges)
      }

    }
    ssc.start()
    ssc.awaitTermination()
  }

}
