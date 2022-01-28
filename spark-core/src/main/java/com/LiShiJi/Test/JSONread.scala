package com.LiShiJi.Test

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.json.JSON
object JSONread {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JSONread").setMaster("local")
    val sc = new SparkContext(conf)
    val jsonStrs = sc.textFile("file:///home/hadoop/app/spark/examples/src/main/resources/people.json")
    val result = jsonStrs.map(s => (JSON).parseFull(s))
    result.foreach( {r => r match {
      case Some(map: Map[String,Any]) => println(map)
      case None => println("Parasing failed")
      case other => println("Uknown data structure:"+other)
    }}
    )
  }
}