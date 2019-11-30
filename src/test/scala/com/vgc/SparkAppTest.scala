package com.vgc

import org.junit.jupiter.api.Test
import com.vgc.SparkApp
import org.junit.jupiter.api.Assertions._


class AppTest {
  @Test private[vgc] def appHasAGreeting(): Unit = {
    val classUnderTest = SparkApp
    assertNotNull(classUnderTest.getGreeting, "app should have a greeting")
  }
}
