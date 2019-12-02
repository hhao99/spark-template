package com.vgc

import org.junit.jupiter.api.Test
import com.vgc.SparkApp
import org.junit.jupiter.api.Assertions._


class AppTest extends InitSpark {
  @Test private[vgc] def appGetDS(): Unit = {
    val app = SparkApp
    assertEquals(app.getDS().count(),12, "ds should get 12 records")
  }
}
