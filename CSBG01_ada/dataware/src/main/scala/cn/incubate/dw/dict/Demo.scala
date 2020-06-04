package cn.incubate.dw.dict

import org.apache.spark.sql.SparkSession

object Demo {
  def main(args: Array[String]): Unit = {
    SparkSession.builder()
      .master("local[*]")
      .appName("demo")
      .getOrCreate()



  }

}
