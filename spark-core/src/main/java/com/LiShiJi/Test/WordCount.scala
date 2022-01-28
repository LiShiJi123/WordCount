package com.LiShiJi.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //
    val sparkconf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sc = new SparkContext(sparkconf)
    //业务实现
    val fileRDD: RDD[String] = sc.textFile("D:\\DATA\\LI-hadoop\\data\\a.txt")
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_+_)
    val word2Count: Array[(String, Int)] = word2CountRDD.collect()
    word2Count.foreach(println)
    //关闭
    sc.stop()
  }
  //

}

