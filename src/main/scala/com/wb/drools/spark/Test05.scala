package com.wb.drools.spark

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.wb.drools.spark.redis.RedisUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Encoders, SparkSession}
import org.kie.api.KieServices
import org.kie.api.command.Command
import org.kie.server.api.marshalling.MarshallingFormat
import org.kie.server.client.{KieServicesFactory, RuleServicesClient}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


object Test05 {

  val url = "http://XXX:8888/kie-server/services/rest/server"
  val user = "kieserver"
  val password = "kieserver1!"
  val kie_container_id = "test01_1.0.0"
  val kie_session_id = "session01"

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)
    //创建spark执行上下文环境
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.streaming.checkpointLocation", "/tmp/spark/Test04")
      .setMaster("local[*]")

    val spark = SparkSession.builder().appName("Test02").config(conf).getOrCreate()


    import spark.implicits._
    val df1 = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "XXX")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]


    implicit val encoder2 = Encoders.bean(classOf[Object])

    df1
      .repartition(30)
      .mapPartitions(x => {
        val jedis = RedisUtil.getJedis
        val temp = x.map(
          y => {
            //str=TestJavaClass.classString
            val str = jedis.get("javaString")
            var classString2: String = ""
            if (str.split(";")(0).contains("package")) { // length + 1 多一个;
              classString2 = str.substring(str.split(";")(0).length + 1)
            }
            else classString2 = str
            if ("".equals(classString2)) {
            }
            val person = TestJavaClass.createStudent("Person", classString2)
            JSON.parseObject(y.toString, person.getClass)
          }
        )
        if (null != jedis) {
          jedis.close()
        }
        temp
      })
      .flatMap(
        x => {
          val FORMAT = MarshallingFormat.JSON
          val conf = KieServicesFactory.newRestConfiguration(url, user, password)
          conf.setMarshallingFormat(FORMAT)
          println(conf)
          println(KieServicesFactory.newKieServicesClient(conf).getClass)
          val kieServicesClient = KieServicesFactory.newKieServicesClient(conf)

          val rulesClient = kieServicesClient.getServicesClient(classOf[RuleServicesClient])
          val commandsFactory = KieServices.Factory.get.getCommands

          val commands: java.util.LinkedList[Command[_]] = new java.util.LinkedList[Command[_]]()
          commands.add(commandsFactory.newInsert(x, "person"))
          commands.add(commandsFactory.newFireAllRules)
          val executeResponse = rulesClient.executeCommandsWithResults(kie_container_id, commandsFactory.newBatchExecution(commands, kie_session_id))
          val value: java.util.LinkedHashMap[Object, util.LinkedHashMap[Object, Object]] = executeResponse.getResult.getValue("person").asInstanceOf[java.util.LinkedHashMap[Object, util.LinkedHashMap[Object, Object]]]
          val resultString = JSON.toJSONString(value.values(), SerializerFeature.WriteMapNullValue)
          val list = JSON.parseObject(resultString, new java.util.ArrayList[Object]().getClass)
          list.map(JSON.toJSONString(_, SerializerFeature.WriteMapNullValue))
        }
      ).createOrReplaceTempView("test")


    spark.sql("select * from test")
      .writeStream
      .option("truncate", false)
      .outputMode(OutputMode.Update())
      .format("console")
      .start()

    spark
      .sql("select * from test")
      .writeStream.outputMode(OutputMode.Update()).format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "XXX")
      .start()
      .awaitTermination()
  }

}
