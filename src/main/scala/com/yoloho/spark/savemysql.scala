package com.yoloho.spark

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yoloho.util.LogURLUtil
import org.apache.rocketmq.common.message.Message
import org.apache.rocketmq.spark.{RocketMQConfig, RocketMqUtils}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.api.java.{JavaInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

case class BdmDayimaSearchLog(var uid: String, var search_time: String, var keyword: String, var last_id: String)

object savemysql {
  private var searcday_time: String = _

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("savemysql").setMaster("local[4]")
    val context: JavaSparkContext = new JavaSparkContext(conf)
    context.setLogLevel("WARN")
    val jsc: JavaStreamingContext = new JavaStreamingContext(context, Seconds(5))

    val sparksession: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()


    val prop: Properties = new Properties()
    val nameServerAddr: String = "10.10.99.205:9876;10.10.99.206:9876;10.10.99.207:9876;10.10.99.208:9876"
    val group: String = "bigDataConsumer2"
    val topic: String = "log_forward"
    val tag: String = "forumapi.yoloho.com"
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    prop.setProperty(RocketMQConfig.NAME_SERVER_ADDR, nameServerAddr)
    prop.setProperty(RocketMQConfig.CONSUMER_MESSAGES_ORDERLY, "false")
    prop.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, "latest")
    prop.setProperty(RocketMQConfig.CONSUMER_GROUP, group)
    prop.setProperty(RocketMQConfig.CONSUMER_TOPIC, topic)
    prop.setProperty(RocketMQConfig.CONSUMER_TAG, tag) //手动设置tag

    val jDstream: JavaInputDStream[Message] = RocketMqUtils.createJavaMQPushStream(jsc, prop, StorageLevel.MEMORY_AND_DISK)
    val stream: InputDStream[Message] = jDstream.inputDStream
    val DS: DStream[String] = stream.map(msg => {
      new String(msg.getBody)
    })
    val dsf: DStream[String] = DS.filter(msg => {
      msg.contains("/group/group/") && msg.contains("keyword=")
    })
    //过滤出需要的字段
    val BdmValue: DStream[BdmDayimaSearchLog] = dsf.map(msg => {
      val string1 = msg.toString
      val nObject: JSONObject = JSON.parseObject(string1)
      //url
      val url = nObject.get("url").toString
      //用户uid
      val uid = LogURLUtil.getUIDFromURLToken(url).toString
      //搜索内容post_data
      val post_data = nObject.get("post_data").toString
      val stringToString = LogURLUtil.getParamMap(post_data)
      //搜索词keyword
      val keyword = stringToString.get("keyword")
      //搜索分页idlastId
      val lastId = stringToString.get("lastId")
      //搜索时间
      val time = nObject.get("time").toString
      // tranTimeToString.formatted(date.getTime)
      val standTime = tranTimeToString(time)

      val bdmlog: BdmDayimaSearchLog = BdmDayimaSearchLog(uid, standTime, keyword, lastId)
      if (bdmlog.keyword != null) {
        if (bdmlog.last_id == null) {
          bdmlog.last_id = "0"
        }
      }
      bdmlog
    }
    )

    BdmValue.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {

        rdd.foreach(rdd => {
          searcday_time = rdd.search_time
        })
        val rdd1: RDD[BdmDayimaSearchLog] = rdd.coalesce(1, false)
        import sparksession.implicits._
        val frame: DataFrame = rdd1.toDF()
        frame.show()

        val url="jdbc:mysql://localhost:3306/spark"

        val tablename="search"
        val properties = new Properties()
        properties.setProperty("user","root")
        properties.setProperty("password","dayima")

        frame.write.mode("append").jdbc(url,tablename,properties)

        // frame.printSchema()
        frame.registerTempTable("table1")
        val str1: String = searcday_time.substring(0, 10)
        //sparksession.sql("select * from table1").show()
        val insertstr = "insert into bdm.dayima_search_log partition(etl_date='" + str1 + "') select uid,search_time,keyword,last_id from table1"
        //保存到hive表格中
        println(insertstr + "测试")
        //sparksession.sql(insertstr)

      }

    })


    jsc.start()
    jsc.awaitTermination()

  }


  //修改时间格式为标准时间
  def tranTimeToString(tm: String): String = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val date: Date = format.parse(tm)
    val ss: String = date.getTime.toString
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(ss.toLong))
    tim
  }
}