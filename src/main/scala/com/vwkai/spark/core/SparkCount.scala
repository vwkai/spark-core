package com.vwkai.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WorkCount")
    val sc = new SparkContext(conf)
    val resultRdd: RDD[(String, Int)] = sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    resultRdd.saveAsTextFile(args(1))
    sc.stop()
  }
}