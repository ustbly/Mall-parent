package com.sdust.mall.realtime.util

import com.alibaba.fastjson.JSONObject

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, Statement}
import scala.collection.mutable.ListBuffer

object MySQLUtil {
  def main(args: Array[String]): Unit = {
    val list: List[JSONObject] = queryList("select * from offset_2021")
    println(list)
  }

  def queryList(sql: String): List[JSONObject] = {
    //注册驱动
    Class.forName("com.mysql.jdbc.Driver")
    val resultList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
    //建立连接
    val conn: Connection = DriverManager.getConnection(
      "jdbc:mysql://hadoop102:3306/mall2021_rs?characterEncoding=utf-8&useSSL=false",
      "root",
      "123456")
    //创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //println(sql)
    val rs: ResultSet = ps.executeQuery()
    val md: ResultSetMetaData = rs.getMetaData
    //处理结果集
    while (rs.next) {
      val rowData = new JSONObject();
      for (i <- 1 to md.getColumnCount) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList += rowData
    }
    rs.close()
    ps.close()
    conn.close()
    resultList.toList
  }

}