package com.vgc
import java.sql.Date

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkApp extends InitSpark with RTMUtil {

    def getDS(): DataFrame = {
        val file = "file:///c:/data/r1.json"
        val df = reader.json(file)
        df
    }
    def getBrake_Step_Info(): DataFrame = {
        val df = getDS().select("vin","pkgTs","data.v050.val","data.v050.val")
            .toDF("vin","pkgts","step","brake")
        df.filter(col("step") === "1" and  col("brake") === "1")

    }


    def main(args: Array[String]): Unit = {
        val df = getBrake_Step_Info();

        df.foreach( r=> {
            val vin = r.getAs[String]("vin")
//            val pkgts = fmt_time.format(new Date(r.getAs[Int]("pkgts")))
            log.info(vin )



        })
      }
}
