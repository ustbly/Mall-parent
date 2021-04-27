package com.sdust.mall.realtime.util

import java.util

import com.sdust.mall.realtime.bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder

/**
 * 操作ES客户端的工具类
 */
object MyESUtil {


  //声明Jest客户端工厂
  private var jestClientFactory: JestClientFactory = null


  //提供获取Jest客户端的方法
  def getJestClient(): JestClient = {
    if (jestClientFactory == null) {
      //创建Jest客户端工厂对象
      build()
    }
    val client: JestClient = jestClientFactory.getObject
    client
  }

  def build() = {
    jestClientFactory = new JestClientFactory
    jestClientFactory.setHttpClientConfig(
    new HttpClientConfig
      .Builder("http://hadoop102:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(10000).build())
  }

  /**
   * 向ES中插入单条数据 方式1  将插入文档的数据直接以json格式传入
   */
  def putIndex_1(): Unit = {
    //获取客户端连接
    val jestClient: JestClient = getJestClient()

    //定义执行的source
    var source =
    """
      |{
      |  "id": 100,
      |  "name": "operation red sea",
      |  "doubanScore": 8.5,
      |  "actorList": [
      |    {
      |      "id": 1,
      |      "name": "zhang yi"
      |    },
      |    {
      |      "id": 2,
      |      "name": "hai qing"
      |    },
      |    {
      |      "id": 3,
      |      "name": "zhang han yu"
      |    }
      |  ]
      |}
      |""".stripMargin

    //创建插入类 Index,Builder中的参数表示要插入到索引中的文档，底层会转换Json格式的字符串
    //也可以将Json类型的字符串文档转换为样例类对象
    val index = new Index.Builder(source)
      .index("movie_index_ly")
      .`type`("movie")
      .id("1")
      .build()
    //通过客户端操作ES，execute参数为Action类型，index是Action接口的一个实现类对象
    jestClient.execute(index)

    //关闭连接
    jestClient.close()
  }

  /**
   * 向ES中插入单条数据 方式2  将插入的文档封装为样例类对象
   */
  def putIndex_2(): Unit = {
    val jestClient: JestClient = getJestClient()

    val actorList = new util.ArrayList[util.Map[String, Any]]()
    val actorMap1 = new util.HashMap[String, Any]()
    actorMap1.put("id", 66)
    actorMap1.put("name", "贾玲")

    actorList.add(actorMap1)


    //封装样例类对象
    val movie: Movie = Movie(200, "hi mom", 8.9f, actorList)
    //创建Action实现类 ==> Index
    val index: Index = new Index.Builder(movie)
      .index("movie_index_ly")
      .`type`("movie")
      .id("2")
      .build()

    jestClient.execute(index)
    //关闭客户端
    jestClient.close()
  }

  /**
   * 根据id获取单条文档
   */
  def queryIndexById(): Unit = {
    val jestClient: JestClient = getJestClient()

    val get: Get = new Get.Builder("movie_index_ly", "2").build()

    val result: DocumentResult = jestClient.execute(get)
    println(result.getJsonString)
    jestClient.close()
  }


  /**
   * 根据查询条件从ES中获取多个文档 方式1
   */
  def queryIndexByCondition1(): Unit = {
    val jestClient: JestClient = getJestClient()

    var query: String =
    """
      |{
      |  "query": {
      |    "bool": {
      |      "must": [
      |        {
      |          "match": {
      |            "name": "red"
      |          }
      |        }
      |      ],
      |      "filter": {
      |        "term": {
      |          "actorList.id": "3"
      |        }
      |      }
      |    }
      |  }
      |}
      |""".stripMargin

    val search: Search = new Search.Builder(query)
      .addIndex("movie_index")
      .addType("movie")
      .build()

    //执行操作
    val result: SearchResult = jestClient.execute(search)

    //获取命中的结果 sourceType:对命中的数据进行封装，因为是 Json，所以我们用 map 封装
    //注意：一定得是 Java 的 Map 类型
    val rsList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])

    //将 Java 转换为 Scala 集合，方便操作
    import scala.collection.JavaConverters.asScalaBufferConverter
    //获取 Hit 中的 source 部分
    val list: List[util.Map[String, Any]] = rsList.asScala.map(_.source).toList
    println(list.mkString("\n"))

    //关闭连接
    jestClient.close()
  }


  /**
   * 根据查询条件从ES中获取多个文档 方式2
   */
  def queryIndexByCondition2(): Unit = {
    val jestClient: JestClient = getJestClient()

    //SearchSourceBuilder 用于构建查询的json格式字符串
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()

    val boolQueryBuilder: BoolQueryBuilder = new BoolQueryBuilder()
    searchSourceBuilder.query(boolQueryBuilder)

    boolQueryBuilder.must(new MatchQueryBuilder("name", "red"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.id", "3"))

    val query: String = searchSourceBuilder.query(boolQueryBuilder).toString
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index")
      .addType("movie")
      .build()

    //执行操作
    val result: SearchResult = jestClient.execute(search)

    //获取命中的结果 sourceType:对命中的数据进行封装，因为是 Json，所以我们用 map 封装
    //注意：一定得是 Java 的 Map 类型
    val rsList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])

    //将 Java 转换为 Scala 集合，方便操作
    import scala.collection.JavaConverters.asScalaBufferConverter
    //获取 Hit 中的 source 部分
    val list: List[util.Map[String, Any]] = rsList.asScala.map(_.source).toList
    println(list.mkString("\n"))
    //关闭连接
    jestClient.close()
  }

  /**
   * 将数据批量插入ES中
   * @param infoList
   * @param indexName
   */
  def bulkInsert(infoList: List[(String,Any)], indexName: String): Unit = {

    if (infoList != null && infoList.size != 0) {
      //获取客户端
      val jestClient: JestClient = getJestClient()

      val bulkBuilder: Bulk.Builder = new Bulk.Builder()
      for ((id,dauInfo) <- infoList) {
        val index: Index = new Index.Builder(dauInfo)
          .index(indexName)
          .id(id)
          .`type`("_doc")
          .build()
        bulkBuilder.addAction(index)
      }

      //创建批量操作对象
      val bulk: Bulk = bulkBuilder.build()
      val bulkResult: BulkResult = jestClient.execute(bulk)

      println("向ES中插入了：" + bulkResult.getItems.size() + "条数据")

      //关闭客户端
      jestClient.close()
    }

  }

  def main(args: Array[String]): Unit = {
    //putIndex_1()
    //putIndex_2()
    //queryIndexById()
    //queryIndexByCondition1
    queryIndexByCondition2

  }

  case class Movie(id: Long, name: String, doubanScore: Float, actorList: util.List[util.Map[String, Any]]) {

  }

}
