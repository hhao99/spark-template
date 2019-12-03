package com.vgc

import java.text.SimpleDateFormat

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class VehicleInfo(oem: String, model: String, brand: String, modelYear: String)
trait RTMUtil {
  val fmt_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def getVehcileInfo(vin: String): VehicleInfo ={

    val model:String  = getModel(vin.substring(6,8))
    val oem: String = getOem(vin.substring(0,3),vin.substring(6,8))
    val brand: String = getBrand(vin.substring(0,3),vin.substring(6,8))
    val modelYear: String = getModelYear(vin.substring(9,10),vin.substring(6,7).matches("[0-9]"))



    VehicleInfo(oem,model,brand,modelYear)
  }

  private def getModel(x: String): String = x match {
    case "AU" => "eGolf"
    case "3C" => "Passat GTE"
    case "7L" => "Touareg D PHEV"
    case "GA" => "Q2 E-tron"
    case "4G" => "A6 C7 Phev"
    case "GE" => "E-tron"
    case "4M" => "Q7 Phev"
    case "4K" => "A6 C8 Phev"
    case _ => x
  }
  private def getBrand(x: String,y:String): String = (x,y) match {
    case ("LFV","AU") => "VW"
    case ("LFV","3C") => "VW"
    case ("LFV","7L") => "VW"
    case ("LFV","GA") => "AUDI"
    case ("LFV","4G") => "AUDI"
    case ("LFV","GE") => "AUDI"
    case ("LFV","4M") => "AUDI"
    case ("LFV","4K") => "AUDI"
    case ("LSV",_) => "VW"
    case ("WVW",_) => "VW"
    case ("WAU",_) => "AUDI"
    case _ => x+"_"+y
  }

  private def getOem(x:String, y:String): String = (x, y) match {
    case ("LFV","AU") => "VW_FAW"
    case ("LFV","3C") => "VW_FAW"
    case ("LFV","7L") => "VW_FAW"
    case ("LFV","GA") => "AUDI_FAW"
    case ("LFV","4G") => "AUDI_FAW"
    case ("LFV","GE") => "AUDI_FAW"
    case ("LFV","4M") => "AUDI_FAW"
    case ("LFV","4K") => "AUDI_FAW"
    case ("LSV",_) => "SAIC_VW"
    case ("WVW",_) => "VGIC"
    case ("WAU",_) => "AUDI_FBU"
    case _ => x+"_"+y
  }

  private def getModelYear(x: String,y:Boolean): String = (x,y) match {
    case ("A",false) => "1980"
    case ("B",false) => "1981"
    case ("C",false) => "1982"
    case ("D",false) => "1983"
    case ("E",false) => "1984"
    case ("F",false) => "1985"
    case ("G",false) => "1986"
    case ("H",false) => "1987"
    case ("J",false) => "1988"
    case ("K",false) => "1989"
    case ("L",false) => "1990"
    case ("M",false) => "1991"
    case ("N",false) => "1992"
    case ("P",false) => "1993"
    case ("R",false) => "1994"
    case ("S",false) => "1995"
    case ("T",false) => "1996"
    case ("V",false) => "1997"
    case ("W",false) => "1998"
    case ("X",false) => "1999"
    case ("Y",false) => "2000"
    case ("1",false) => "2001"
    case ("2",false) => "2002"
    case ("3",false) => "2003"
    case ("4",false) => "2004"
    case ("5",false) => "2005"
    case ("6",false) => "2006"
    case ("7",false) => "2007"
    case ("8",false) => "2008"
    case ("9",false) => "2009"

    case ("A",true) => "2010"
    case ("B",true) => "2011"
    case ("C",true) => "2012"
    case ("D",true) => "2013"
    case ("E",true) => "2014"
    case ("F",true) => "2015"
    case ("G",true) => "2016"
    case ("H",true) => "2017"
    case ("J",true) => "2018"
    case ("K",true) => "2019"
    case ("L",true) => "2020"
    case ("M",true) => "2021"
    case ("N",true) => "2022"
    case ("P",true) => "2023"
    case ("R",true) => "2024"
    case ("S",true) => "2025"
    case ("T",true) => "2026"
    case ("V",true) => "2027"
    case ("W",true) => "2028"
    case ("X",true) => "2029"
    case ("Y",true) => "2030"
    case ("1",true) => "2031"
    case ("2",true) => "2032"
    case ("3",true) => "2033"
    case ("4",true) => "2034"
    case ("5",true) => "2035"
    case ("6",true) => "2036"
    case ("7",true) => "2037"
    case ("8",true) => "2038"
    case ("9",true) => "2039"
    case _ => "9999"
  }

}
