package com.sdust.mall.realtime.dwd

import com.alibaba.fastjson
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.sdust.mall.realtime.bean.{OrderInfo, ProvinceInfo, UserInfo, UserStatus}
import com.sdust.mall.realtime.util._
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * 从Kafka中读取订单数据，并对其进行处理
 */
object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("OrderInfoApp").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    var topic = "ods_order_info"
    var groupId = "order_info_group"

    //=================1.从Kafka主题中读取数据============================
    //从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUitl.getOffset(topic, groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    //判断偏移量是否存在来决定从什么位置读取数据
    if (offsetMap != null && offsetMap.nonEmpty) {
      //从指定的偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      //从最新位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获取当前批次处理的偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        rdd
      }
    }

    //对接收到的数据进行数据结构的转换  ConsumerRecord[String, String(jsonStr)] -> jsonObj
    val orderInfoDStream: DStream[OrderInfo] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
        //2021-03-31 10:28:32
        val createTime: String = orderInfo.create_time
        val createTimeArr: Array[String] = createTime.split(" ")
        orderInfo.create_date = createTimeArr(0)
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        orderInfo
      }
    }

    //orderInfoDStream.print(1000)

    //=================2.判断是否为首单============================
    //方案1:对于每条订单都要执行一个sql，sql语句过多
    /*
    val orderInfoWithFirstDStream: DStream[OrderInfo] = orderInfoDStream.map {
      orderInfo => {
        val userId: Long = orderInfo.user_id
        //根据用户Id到Phoenix中查询是否下单过
        var sql: String = s"select user_id,if_consumed from user_status2021 where user_id = '${userId}'"
        val userStatusList: List[fastjson.JSONObject] = PhoenixUtil.queryList(sql)
        if (userStatusList != null && userStatusList.size > 0) {
          orderInfo.if_first_order = "0"
        } else {
          orderInfo.if_first_order = "1"
        }
        orderInfo
      }
    }

    orderInfoWithFirstDStream.print(1000)
    */
    //=================2.判断是否为首单============================
    //方案2:以分区为单位，将整个分区的数据拼接一条sql进行一次查询
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions {
      orderInfoItr => {
        //当前一个分区中所有订单的集合
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList

        //获取当前分区中下订单的用户
        val userIdList: List[Long] = orderInfoList.map(_.user_id)

        //根据用户集合到Phoenix中查询，看一下哪些用户下过单 坑1 -> 字符串拼接
        var sql: String =
          s"select user_id,if_consumed from user_status2021 where user_id in ('${userIdList.mkString("','")}')"

        //执行sql，从Phoenix中获取数据
        val userStatusList: List[fastjson.JSONObject] = PhoenixUtil.queryList(sql)
        //获取消费过的用户id  坑2 -> 大小写
        val consumedUserIdList: List[String] = userStatusList.map(_.getString("USER_ID"))

        for (orderInfo <- orderInfoList) {
          //坑3 -> 类型转换
          if (consumedUserIdList.contains(orderInfo.user_id.toString)) {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      }
    }
    //orderInfoWithFirstFlagDStream.print(1000)
    /*
      ➢ 漏洞
        如果一个用户是首次消费，在一个采集周期中，这个用户下单了 2 次，那么就会把这同一个用户都会统计为首单消费
      ➢ 解决办法
        应该将同一采集周期的同一用户的最早的订单标记为首单，其它都改为非首单
        1. 同一采集周期的同一用户-----按用户分组（groupByKey）
        2. 最早的订单-----排序，取最早（sortwith）
        3. 标记为首单-----具体业务代码
     */
    //=================4.同一批次中状态的修正============================
    //对待处理数据结构进行转换 OrderInfo -> (userId,orderInfo)
    val mapDStream: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDStream.map {
      orderInfo => {
        (orderInfo.user_id, orderInfo)
      }
    }

    //根据用户id对用户进行分组
    val groupByKeyDStream: DStream[(Long, Iterable[OrderInfo])] = mapDStream.groupByKey()

    //
    val orderInfoRealDStream: DStream[OrderInfo] = groupByKeyDStream.flatMap {
      case (user_id, orderInfoItr) => {
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //判断在一个采集周期中，用户是否下了多订单
        if (orderInfoList != null && orderInfoList.size > 1) {
          //如果下了多订单，要按照下单时间升序排序
          val sortedOrderInfoList: List[OrderInfo] = orderInfoList.sortWith {
            (orderInfo1, orderInfo2) => {
              orderInfo1.create_time < orderInfo2.create_time
            }
          }
          //取出集合中第一个元素(最早下的订单)
          if (sortedOrderInfoList(0).if_first_order == "1") {
            //时间最早的订单首单状态保留为1，其他时间的订单首单状态保留为0（非首单）
            for (i <- 1 until sortedOrderInfoList.size) {
              sortedOrderInfoList(i).if_first_order = "0"
            }
          }
          sortedOrderInfoList
        } else {
          orderInfoList
        }
      }
    }

    //=================5.和省份维度表进行关联============================
    //5.1 关联省份方案1：以分区为单位，对订单数据进行处理，和Phoenix中的订单表进行关联
    /*
    val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoRealDStream.mapPartitions {
      orderInfoItr => {
        //转换为List
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取当前分区中订单对应的省份id
        val provinceIdList: List[Long] = orderInfoList.map(_.province_id)
        //根据省份id到Phoenix中查询对应的省份
        val sql: String = s"select id,name,area_code,iso_code from mall2021_province_info where id in ('${provinceIdList.mkString("','")}')"
        val provinceInfoList: List[fastjson.JSONObject] = PhoenixUtil.queryList(sql)

        val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
          provinceJsonObj => {
            //将json对象转换为省份样例类对象
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
            (provinceInfo.id, provinceInfo)
          }
        }.toMap

        //对订单数据进行遍历，用遍历出的省份id，从provinceInfoMap来获取省份对象
        for (orderInfo <- orderInfoList) {
          val proInfo: ProvinceInfo = provinceInfoMap.getOrElse(orderInfo.province_id.toString, null)
          if (proInfo != null) {
            orderInfo.province_name = proInfo.name
            orderInfo.province_area_code = proInfo.area_code
            orderInfo.province_iso_code = proInfo.iso_code
          }
        }
        orderInfoList.toIterator
      }
    }
    */

    //5.2 关联省份方案2：使用广播变量，在Driver端进行一次查询，分区越多效果越明显 前提：省份数据量较小
    //以采集周期为单位对数据进行处理 ----> 通过SQL将所有省份查询出来
    val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoRealDStream.transform {
      rdd => {
        //从Phoenix查询所有的省份数据
        var sql: String = "select id,name,area_code,iso_code from mall2021_province_info"
        val provinceInfoList: List[fastjson.JSONObject] = PhoenixUtil.queryList(sql)
        val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
          provinceJsonObj => {
            val provinceInfo: ProvinceInfo = JSON.toJavaObject(provinceJsonObj, classOf[ProvinceInfo])
            (provinceInfo.id, provinceInfo)
          }
        }.toMap

        //定义省份的广播变量
        val bdMap: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceInfoMap)

        rdd.map {
          orderInfo => {
            val proInfo: ProvinceInfo = bdMap.value.getOrElse(orderInfo.province_id.toString, null)
            if (proInfo != null) {
              orderInfo.province_name = proInfo.name
              orderInfo.province_area_code = proInfo.area_code
              orderInfo.province_iso_code = proInfo.iso_code
            }
            orderInfo
          }
        }

      }
    }

    //orderInfoWithProvinceDStream.print(1000)

    //=================6.和用户维度表进行关联============================
    //以分区为单位，对数据进行处理，每个分区拼接一个sql，到Phoenix上查询用户数据
    val orderInfoWithUserInfoDStream: DStream[OrderInfo] = orderInfoWithProvinceDStream.mapPartitions {
      orderInfoItr => {
        //转换为List集合
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList

        //获取当前分区中所有用户id
        val userIdList: List[Long] = orderInfoList.map(_.user_id)

        //根据id拼接sql语句，到Phoenix中查询用户
        var sql: String = s"select id,user_level,birthday,gender,age_group,gender_name from mall2021_user_info where id in ('${userIdList.mkString("','")}')"

        //当前分区中的所有下单用户
        val userList: List[fastjson.JSONObject] = PhoenixUtil.queryList(sql)
        val userMap: Map[String, UserInfo] = userList.map {
          userJsonObj => {
            val userInfo: UserInfo = JSON.toJavaObject(userJsonObj, classOf[UserInfo])
            (userInfo.id, userInfo)
          }
        }.toMap

        for (orderInfo <- orderInfoList) {
          val userInfoObj: UserInfo = userMap.getOrElse(orderInfo.user_id.toString, null)
          if (userInfoObj != null) {
            orderInfo.user_age_group = userInfoObj.age_group
            orderInfo.user_gender = userInfoObj.gender_name
          }
        }
        orderInfoList.toIterator
      }
    }

    //orderInfoWithUserInfoDStream.print(1000)

    //=================3.维护首单用户状态 || 保存订单到ES中============================
    //如果当前用户为首单用户(第一次消费)，那么我们进行首单标记之后，应该将用户的消费状态保存到Hbase中，
    //等下次这个用户再下单的时候，就不能称作首单了

    import org.apache.phoenix.spark._
    orderInfoWithUserInfoDStream.foreachRDD {
      rdd => {
        //优化：对rdd做一下缓存
        rdd.cache()

        //3.1 维护首单用户状态
        //将首单用户过滤出来
        val firstOrderRDD: RDD[OrderInfo] = rdd.filter(_.if_first_order == "1")

        //注意：在使用saveToPhoenix方法的时候，要求RDD中存放数据的属性个数和Phoenix表中字段的个数要一致

        val userStatusRDD: RDD[UserStatus] = firstOrderRDD.map {
          orderInfo => UserStatus(orderInfo.user_id.toString, "1")
        }

        userStatusRDD.saveToPhoenix(
          "USER_STATUS2021",
          Seq("USER_ID", "IF_CONSUMED"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181")
        )

        //3.2 保存订单到ES中
        rdd.foreachPartition {
          orderInfoItr => {
            val orderInfoList: List[(String, OrderInfo)] = orderInfoItr.toList.map(orderInfo => (orderInfo.id.toString, orderInfo))
            val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(orderInfoList, "mall2021_order_info_" + dateStr)

            //3.4 写回到Kafka
            for ((orderInfoId,orderInfo) <- orderInfoList) {
              MyKafkaSink.send(
                "dwd_order_info",
                JSON.toJSONString(orderInfo,new SerializeConfig(true)))
            }

          }
        }


        //3.3 提交偏移量
        OffsetManagerUitl.saveOffset(topic, groupId, offsetRanges)
      }

    }


    ssc.start()
    ssc.awaitTermination()
  }
}
