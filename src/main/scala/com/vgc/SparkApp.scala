package com.vgc
import org.apache.spark.sql._

object SparkApp extends InitSpark {

    def getDS():DataFrame = {
        val file = "file:///c:/data/r1.json"
        val df = reader.json(file)
        df
    }


    def main(args: Array[String]) {
        println(getDS().count())
    }
}
