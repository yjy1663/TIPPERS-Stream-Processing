package TIPPERSUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

/**
  * Created by zhongzhuojian on 22/05/2017.
  */
object OpHelper {


  def execute(streamMap : mutable.HashMap[String, DStream[Any]], op : Object): Unit = {

    op match {
      case i1 : Joiner =>
        val leftStream = streamMap(i1.leftTableName)
        val rightStream = streamMap(i1.rightTableName)
        val joinedStream = leftStream.transformWith(rightStream, (rdd1 : RDD[Any], rdd2 : RDD[Any]) => rdd1.cartesian(rdd2))
                                       .map(a =>{
                                         val t1 = a._1.asInstanceOf[List[Any]]
                                         val t2 = a._2.asInstanceOf[List[Any]]
                                         (t1 ++ t2).asInstanceOf[Any]
                                       })

      case i2 : Filter =>

      case i3 : Projector =>

      case _ =>

    }
    if(op.isInstanceOf[Joiner]){

    }

    else if(op.isInstanceOf[Filter]){

    }

    else if(op.isInstanceOf[Projector]){

    }
    else if(op.isInstanceOf[Aggregator]){

    }


  }

  def streamRegistration(streamMap : mutable.HashMap[String, DStream[Any]]): Unit ={

  }


}
