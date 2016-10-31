package main

import org.apache.spark.{SparkConf, SparkContext}

object MainApp extends App{
  val sparkcontext = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))



  System.out.println("Spark job complete")
}
