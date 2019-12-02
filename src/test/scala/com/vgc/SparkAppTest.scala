package com.vgc

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions._


class AppTest extends InitSpark {
  import spark.implicits._
  @Test private[vgc] def testGetDS(): Unit = {
    val app = SparkApp
    assertEquals(app.getDS().count(),4 , "ds should get 4 records")
  }

  @Test private[vgc] def testStep(): Unit = {

    val app = SparkApp
    val bsDF = app.getBrake_Step_Info()
    val step_info = bsDF.filter( $"step" === '1' )
    assertEquals(step_info.count() ,2, "step should get 2 records")
  }
}
