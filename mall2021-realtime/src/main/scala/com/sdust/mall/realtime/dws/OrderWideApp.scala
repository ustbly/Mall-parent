package com.sdust.mall.realtime.dws

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.sdust.mall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.sdust.mall.realtime.util.{MyKafkaSink, MyKafkaUtil, MyRedisUtil, OffsetManagerUitl}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.lang
import java.util.Properties
import scala.collection.mutable.ListBuffer

/**
 * 从Kafka的DWD层读取订单和订单明细数据
 * 注意：如果程序数据的来源是Kafka，在程序中如果触发多次行动操作，应该进行缓存
 */
object OrderWideApp {
  def main(args: Array[String]): Unit = {
    //=======================1.从Kafka中获取数据=======================
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderWideApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoTopic = "dwd_order_info"
    val orderInfoGroupId = "dws_order_info_group"

    val orderDetailTopic = "dwd_order_detail"
    val orderDetailGroupId = "dws_order_detail_group"

    //获取偏移量
    val orderInfoOffsetMap: Map[TopicPartition, Long] = OffsetManagerUitl.getOffset(orderInfoTopic, orderInfoGroupId)
    val orderDetailOffsetMap: Map[TopicPartition, Long] = OffsetManagerUitl.getOffset(orderDetailTopic, orderDetailGroupId)

    var orderInfoRecordDStream: InputDStream[ConsumerRecord[String, String]] = null;
    if (orderInfoOffsetMap != null && orderInfoOffsetMap.size > 0) {
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMap, orderInfoGroupId)
    } else {
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoGroupId)
    }

    var orderDetailRecordDStream: InputDStream[ConsumerRecord[String, String]] = null;
    if (orderDetailOffsetMap != null && orderDetailOffsetMap.size > 0) {
      orderDetailRecordDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMap, orderDetailGroupId)
    } else {
      orderDetailRecordDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId)
    }


    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoDStream: DStream[ConsumerRecord[String, String]] = orderInfoRecordDStream.transform {
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailDStream: DStream[ConsumerRecord[String, String]] = orderDetailRecordDStream.transform {
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //orderInfoDStream.map(_.value()).print(1000)
    //orderDetailDStream.map(_.value()).print(1000)

    //转换为kv结构便于join
    val orderInfoDS: DStream[OrderInfo] = orderInfoDStream.map {
      record => {
        val orderInfoStr: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
        orderInfo
      }
    }

    val orderDetailDS: DStream[OrderDetail] = orderDetailDStream.map {
      record => {
        val orderDetailStr: String = record.value()
        val orderDetail: OrderDetail = JSON.parseObject(orderDetailStr, classOf[OrderDetail])
        orderDetail
      }
    }

    //orderInfoDS.print(1000)
    //orderDetailDS.print(1000)
    /*
    以下写法是错误的
    val orderInfoWithKeyDStream: DStream[(Long,OrderInfo)] = orderInfoDStream.map {
      record => {
        val orderInfoJsonStr: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(orderInfoJsonStr, classOf[OrderInfo])
        (orderInfo.id,orderInfo)
      }
    }

    val orderDetailWithKeyDStream: DStream[(Long,OrderDetail)] = orderDetailDStream.map {
      record => {
        val orderDetailJsonStr: String = record.value()
        val orderDetail: OrderDetail = JSON.parseObject(orderDetailJsonStr, classOf[OrderDetail])
        (orderDetail.order_id,orderDetail)
      }
    }

    val joinDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream)
    */

    //=======================2.双流join=======================
    //开窗
    val orderInfoWindowDStream: DStream[OrderInfo] = orderInfoDS.window(Seconds(50), Seconds(5))
    val orderDetailWindowDStream: DStream[OrderDetail] = orderDetailDS.window(Seconds(50), Seconds(5))

    val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoWindowDStream.map {
      orderInfo => {
        (orderInfo.id, orderInfo)
      }
    }

    val orderDetailWithKeyDStream: DStream[(Long, OrderDetail)] = orderDetailWindowDStream.map {
      orderDetail => {
        (orderDetail.order_id, orderDetail)
      }
    }

    //双流join
    val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream, 4)

    //去重  Redis type:Set key:order_join:[orderId] value:orderDetailId expire:60*10
    val orderWideDStream: DStream[OrderWide] = joinedDStream.mapPartitions {
      tupleItr => {
        //获取Jedis客户端
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        val orderWideList = new ListBuffer[OrderWide]
        for ((orderId, (orderInfo, orderDetail)) <- tupleItr) {
          val orderKey: String = "order_join" + orderId
          val isNotExists: lang.Long = jedis.sadd(orderKey, orderDetail.id.toString)
          jedis.expire(orderKey, 600)
          if (isNotExists == 1L) {
            orderWideList.append(new OrderWide(orderInfo, orderDetail))
          }
        }
        jedis.close()
        orderWideList.toIterator
      }
    }

    //orderWideDStream.print(1000)

    //=======================3.实付分摊=======================
    val orderWideSplitDStream: DStream[OrderWide] = orderWideDStream.mapPartitions {
      orderWideItr => {
        val orderWideList: List[OrderWide] = orderWideItr.toList
        //获取Jedis连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        for (orderWide <- orderWideList) {
          //3.1 从Redis中获取明细累加
          var orderOriginSumKey = "order_origin_sum:" + orderWide.order_id
          var orderOriginSum: Double = 0D
          val orderOriginSumStr: String = jedis.get(orderOriginSumKey)
          //注意：从Redis中获取字符串，要进行非空判断
          if (orderOriginSumStr != null && orderOriginSumStr.size > 0) {
            orderOriginSum = orderOriginSumStr.toDouble
          }

          //3.1 从Redis中获取实付分摊累加和
          var orderSplitSumKey = "order_split_sum:" + orderWide.order_id
          var orderSplitSum: Double = 0D
          val orderSplitSumStr: String = jedis.get(orderSplitSumKey)
          if (orderSplitSumStr != null && orderSplitSumStr.size > 0) {
            orderSplitSum = orderSplitSumStr.toDouble
          }

          val detailAmount: Double = orderWide.sku_price * orderWide.sku_num
          //判断是否是最后一条 计算实付分摊
          if (detailAmount == orderWide.original_total_amount - orderOriginSum) {
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - orderSplitSum) * 100d) / 100d
          } else {
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount * detailAmount / orderWide.original_total_amount) * 100d) / 100d
          }
          //3.4更新Redis中的值
          var neworderOriginSum = orderOriginSum + detailAmount
          jedis.setex(orderOriginSumKey, 600, neworderOriginSum.toString)

          var neworderSplitSum = orderSplitSum + orderWide.final_detail_amount
          jedis.setex(orderSplitSumKey, 600, neworderSplitSum.toString)
        }
        //关闭连接
        jedis.close()
        orderWideList.toIterator
      }
    }

    //orderWideSplitDStream.print(1000)

    //向ClickHouse中保存数据
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().appName("spark_sql_orderWide").getOrCreate()
    //对DS中的RDD进行处理
    import spark.implicits._
    orderWideSplitDStream.foreachRDD {
      rdd => {
        //将rdd进行缓存
        rdd.cache()

        //将数据保存到ClickHouse
        val df: DataFrame = rdd.toDF
        df.write.mode(SaveMode.Append)
          .option("batchsize", "100")
          .option("isolationLevel", "NONE") // 设置事务
          .option("numPartitions", "4") // 设置并发
          .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
          .jdbc("jdbc:clickhouse://hadoop102:8123/default",
            "t_order_wide_2021",
            new Properties())

        //将数据写回到Kafka的 dws_order_wide
        rdd.foreach {
          orderWide => {
            MyKafkaSink.send("dws_order_wide",JSON.toJSONString(orderWide,new SerializeConfig(true)))

          }
        }

        //提交偏移量
        OffsetManagerUitl.saveOffset(orderInfoTopic,orderInfoGroupId,orderInfoOffsetRanges)
        OffsetManagerUitl.saveOffset(orderDetailTopic,orderDetailGroupId,orderDetailOffsetRanges)

      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
