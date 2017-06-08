package cs223new

import TIPPERSUtils.TIPPERSVariableDeclaration
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime

import scala.collection.{JavaConversions, mutable}
import scala.collection.mutable.HashMap


/**
  * Created by zhongzhuojian on 20/05/2017.
  */
object TIPPERSQueryEngine {
  object Model {

    final case class SampleModel(timeStamp: String, sensor_id: String, observation_type_id: String,
                                    id: Int, client_id: Int)

  }

  def main(args: Array[String]) {

    val kafkaBrokers = "localhost:9092"

    val topics = "sample"

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("sample test").getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")
//    val schema2 = new StructType()
//      .add("id", IntegerType).add("payload", new StructType().add("client_id", StringType))
//      .add("timeStamp", TimestampType).add("sensor_id", StringType).add("observation_type_id", IntegerType)

    val schema2 = new StructType().add("timeStamp", TimestampType)
                                  .add("sensor_id", StringType)
                                  .add("observation_type_id", StringType)
                                  .add("id", StringType)
                                  .add("payload", new StructType().add("client_id", StringType))
//    # value schema: { "a": 1, "b": "string" }
//    schema = StructType().add("a", IntegerType()).add("b", StringType())
//    df.select( \
//      col("key").cast("string"),
//      from_json(col("value").cast("string"), schema))
    //val ds1 = spark.readStream.schema(schema2).csv("/Users/zhongzhuojian/Documents/TIPPERSdatacsv")
    val ds1 = spark//.read.json("/Users/zhongzhuojian/Documents/TIPPERSdata/TestWiFiAP2.json")
      .readStream.schema(schema2)
      .json("/Users/zhongzhuojian/Documents/TIPPERSdata")
      //.format("kafka")
//      .option("kafka.bootstrap.servers", kafkaBrokers)
//      .option("subscribe", topics)
//      .option("startingOffsets", "earliest")

      //.load()
    //ds1.printSchema()
//    ds1.createOrReplaceTempView("d1t")
//    val ds1t = spark.sql("select * from d1t")
//                    .writeStream
//                    .format("console")
//                    .trigger(ProcessingTime("0 seconds"))
//                    .start()
//                    .awaitTermination(600000)

    import spark.implicits._
    var ds2 = ds1
    if(args(0) != "count"){
      ds2 = ds1.withWatermark("timeStamp", "1 minutes")
        .groupBy(window($"timeStamp", "3 minutes", "1 minutes"), $"id", $"payload.client_id", $"timeStamp", $"sensor_id", $"observation_type_id")
        .count()//.orderBy("window")
    }
    else {
      ds2 = ds1.withWatermark("timeStamp", "1 second")
        .groupBy(window($"timeStamp", "3 minutes", "1 minutes"))
        .count()//.orderBy("window")
    }

    ds2.printSchema()
    val sensorMap = new HashMap[String, DataFrame]
    val observationMap = new HashMap[String, DataFrame]

    initializeMapping(sensorMap, observationMap)
    ds2.createOrReplaceTempView("sgr")
    spark.sql("select * from sgr test").writeStream.outputMode("complete")
      .format("console").start().awaitTermination(300000)
//    ds2.writeStream.outputMode("complete")
//    .format("console")
//      //.trigger(ProcessingTime(200000))
//      .option("truncate", false)
//      //.format("json")
//      //.option("checkpointLocation", "checkpoint/")
//      //.option("path", "output/")
//        //.option("numRows", 100)
//    .queryName("test")
//    .start().awaitTermination(300000)

//    if(args(0) == "count") {
//      val ds3 = spark.sql("select * from test")
//        .writeStream
//        .format("console")
//        .outputMode("complete")
//        .trigger(ProcessingTime("0 seconds"))
//        .start()
//        .awaitTermination(600000)
//    }
//    else{
//      val ds3 = spark.sql("select * from test")
//        .writeStream
//        .format("console")
//        .outputMode("complete")
//        .trigger(ProcessingTime("0 seconds"))
//        .start()
//        .awaitTermination(600000)
//    }


  }




  def initializeMapping(sensorMap: HashMap[String, DataFrame], observationMap : HashMap[String, DataFrame]): Unit ={
    for(a <- JavaConversions.asScalaBuffer(TIPPERSVariableDeclaration.getSensors))
      sensorMap.put(a, null)
    for(a <- JavaConversions.asScalaBuffer(TIPPERSVariableDeclaration.getObservations))
      observationMap.put(a, null)
  }


  def reshape(ds1 : DataFrame): DataFrame ={
//    ds1.select(split(ds1.col("value").cast("string"), ", ").getItem(0).cast("string").as("timeStamp"),
//      split(ds1.col("value").cast("string"), ", ").getItem(1).cast("string").as("sensor_id"),
//      split(ds1.col("value").cast("string"), ", ").getItem(2).cast("string").as("observation_type_id"),
//      split(ds1.col("value").cast("string"), ", ").getItem(3).cast("integer").as("id"),
//      split(ds1.col("value").cast("string"), ", ").getItem(4).cast("integer").as("client_id"))
    ds1.select(split(ds1.col("value").cast("string"), ", ").getItem(0).cast("string").as("timeStamp"),
      split(ds1.col("value").cast("string"), ", ").getItem(1).cast("string").as("sensor_id"),
      split(ds1.col("value").cast("string"), ", ").getItem(2).cast("integer").as("observation_type_id"),
      split(ds1.col("value").cast("string"), ", ").getItem(3).cast("string").as("id"),
      split(ds1.col("value").cast("string"), ", ").getItem(4).cast("string").as("client_id"))
  }









  def reshape(a : Row) = {
    val b = a.toSeq

    val c =  b(0).toString.split(", ")



  }







}
