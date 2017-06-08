package cs223new

import TIPPERSUtils.TIPPERSVariableDeclaration
import org.apache.spark.sql.functions.{split, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, _}

import scala.collection.JavaConversions
import scala.collection.mutable.HashMap


/**
  * Created by zhongzhuojian on 20/05/2017.
  */
object TIPPERSQueryEngineNew {
  object Model {

    final case class SampleModel(timeStamp: String, sensor_id: String, observation_type_id: String,
                                    id: Int, client_id: Int)

  }

  def main(args: Array[String]) {


    //[0] : tableName
    //[1] : aggregate
    //[2] : sql
    //[3] : window size
    //[4] : slide
    //[5] : duration
    //[6] : topic
    //[7] : app name
    val tableName = args(0)
    val aggregate = args(1)
    val aggColumn = args(2)
    val groupByCol = args(3)
    val sql = args(4)
    val windowSize = args(5)
    val slide = args(6)
    val duration = args(7) match {
      case "" => "10000000"
      case _ => args(7)
    }
    val topics = args(8)
    val appName = args(9)
    val whereClause = args(10)

    val kafkaBrokers = "localhost:9092"
    println("duration is" + duration)
    //val topics = "sample"

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
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

    var ds2 = whereClause match {
      case "" => ds1
      case _ => ds1.where(whereClause)
    }
    if(aggregate != ""){
      val tds = groupByCol match {
        case "" => ds2.withWatermark("timeStamp", windowSize).groupBy(window($"timeStamp", windowSize, slide))
        case _ =>
                    val ttt = Array(window($"timeStamp", windowSize, slide)) ++ groupByCol.split(",").map(a => new Column(a))
                    println(ttt)
                    ds2.withWatermark("timeStamp", windowSize)
                        .groupBy(ttt : _*)

      }

      ds2 = aggregate match {
        case "count(*)" => tds.count().orderBy("window")
        case "max" => tds.max(aggColumn).orderBy("window")
        case "min" => tds.min(aggColumn).orderBy("window")
        case "mean" => tds.mean(aggColumn).orderBy("window")
        case "sum" => tds.sum(aggColumn).orderBy("window")

      }
      ds2.writeStream.outputMode("complete")
        .format("console")
        .option("numRows", 10000)
        .option("truncate", false)
        .queryName(tableName)
        .start().awaitTermination(duration.toLong)




    }
    else {
      ds2 = ds1.withWatermark("timeStamp", windowSize)
        .groupBy(window($"timeStamp", windowSize, slide), $"id", $"payload", $"timeStamp", $"sensor_id", $"observation_type_id")
        .count().orderBy("window")
      ds2.createOrReplaceTempView(tableName)
      spark.sql(sql).writeStream.outputMode("complete")
        .format("console")
        .option("numRows", 10000)
        .option("truncate", false)
        .queryName(tableName)
        .start().awaitTermination(duration.toLong)
    }

    //ds2.printSchema()
    //val sensorMap = new HashMap[String, DataFrame]
    //val observationMap = new HashMap[String, DataFrame]

    //initializeMapping(sensorMap, observationMap)
    //ds2.createOrReplaceTempView("test")

//    ds2.writeStream.outputMode("complete")
//    .format("console")
//    .queryName(tableName)
//    .start().awaitTermination(1000 * duration.toLong)
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
