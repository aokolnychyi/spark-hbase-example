package com.aokolnychyi.spark.hbase.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object HBaseExample {

  case class Record(col0: Int, col1: Int, col2: Boolean)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark HBase Example")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    def catalog =
      s"""{
          |"table":{"namespace":"default", "name":"table1"},
          |"rowkey":"key",
          |"columns":{
          |"col0":{"cf":"rowkey", "col":"key", "type":"int"},
          |"col1":{"cf":"cf1", "col":"col1", "type":"int"},
          |"col2":{"cf":"cf2", "col":"col2", "type":"boolean"}
          |}
          |}""".stripMargin

    val artificialData = (0 to 100).map(number => Record(number, number, number % 2 == 0))

    spark
      .createDataFrame(artificialData)
      .write
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    val df = spark
      .read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    df.where($"col2" === true).show()

  }

}
