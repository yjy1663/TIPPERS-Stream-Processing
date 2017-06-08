//package cs223new
//
//import TIPPERSUtils.OpHelper
//
//import kafka.serializer.StringDecoder
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//
//import scala.collection.mutable.ArrayBuffer
//
//import java.util.ArrayList
//
///**
//  * Created by zhongzhuojian on 20/05/2017.
//  */
//object StreamEngine {
//
//  def main(args: Array[String]): Unit ={
//    val kafkaBrokers = "localhost:9092"
//    val topics = "sample"
//    val timeout = 2
//
//    val batchDuration: Duration = Milliseconds(250)
//
//
//    val conf = new SparkConf(true)
//        .set("spark.driver.allowMultipleContexts", "true")
//        .setAppName("test").setMaster("local[4]")
//
//    val sc = new SparkContext(conf)
//    val ssc = new StreamingContext(sc, batchDuration)
//    ssc.checkpoint("checkpoint23")
//
//    val topicsSet = topics.split(",").toSet
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokers)
//
//    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
//                              .window(Milliseconds(500), Milliseconds(250))
//    val lines: DStream[String] = messages.map(_._2)
////    val rddbuf = new ArrayBuffer[Any]
////    lines.getClass
////    val cn = Class.forName("scala.collection.mutable.ArrayBuffer")
////    val ei = rddbuf(1).asInstanceOf[String.type ]
////    val b = new ArrayList[Integer]
////
////
////    if(b.isInstanceOf[ArrayList]){
////
////    }
//
//    val spark = SparkSession.builder.config(conf).getOrCreate()
//    val result = lines.map(a =>{
//      val b = a.split(", ")
//      (b(0), b(1), b(2).toInt, b(3), b(4))
//    })
//      .foreachRDD(rdds =>{
//        //rddbuf.append(a)
////        val rddst = rdds.cartesian(rdds).map(a =>
////          (a._1._1, a._1._2, a._1._3, a._1._4, a._1._5, a._2._1, a._2._2, a._2._3, a._2._4, a._2._5))
////        val spark = SparkSession.builder.config(rddst.sparkContext.getConf).getOrCreate()
//        import spark.implicits._
//
//        val df = rdds.toDF("timeStamp", "sensor_id", "observation_type_id", "id", "client_id")
//
//
//        df.createOrReplaceTempView("test")
//        val dff = spark.sql("select count(*) from test")//.show()
//
//
//      })
//
//
////    val rdds = sc.union(rddbuf)
////    val spark = SparkSession.builder.config(rdds.sparkContext.getConf).getOrCreate()
////    import spark.implicits._
////
////    val df = rdds.toDF("timeStamp", "sensor_id", "observation_type_id", "id", "client_id")
////
////    df.createGlobalTempView("test")
////    spark.sql("select count(*) from global_temp.test").show()
//
//
//    ssc.start()
//    ssc.awaitTerminationOrTimeout(Minutes(timeout.toLong).milliseconds)
//    ssc.stop(stopSparkContext = true, stopGracefully = true)
//  }
//
//
//}
//
