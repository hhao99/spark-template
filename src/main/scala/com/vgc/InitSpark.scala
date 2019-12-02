package com.vgc
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql._

trait InitSpark {
  val spark = SparkSession.builder()
    .appName("appName")
    .config("sparksomeconf","value")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext

  def reader = spark.read
    .option("header","true")
    .option("inferSchema","true")
//    .option("mode","DROPMALFORMED")

  private def init(): Unit = {
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com.vgc").setLevel(Level.INFO)

    LogManager.getRootLogger().setLevel(Level.ERROR)

  }

  init

  def close(): Unit = {
    spark.close()
  }
}
