package org.dummy

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._


object ScalaApp extends App{

  //Suppress Spark output
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val sc = new SparkContext("local", "Simple", "$SPARK_HOME"
    , List("target/spark-sql-training-1.0.jar"))
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

 //Athlete,Age,Country,Game,Date,Sport,Gold,Silver,Bronze,Total

  import org.dummy.ScalaApp.sqlContext.implicits._
  val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    .load("OlympicAthletes.csv")

  df.registerTempTable("Records")



  df.select("Country", "Total").groupBy("Country").agg(sum($"Total"), avg($"Total"))
    .foreach(println)


  sqlContext.sql("SELECT Country,count(1) from Records where Age > 40 group by Country")
    .foreach(println)

  sqlContext.udf.register("strLen", (s: String) => s.length())
  sqlContext.sql("SELECT Country, strLen(Country) from Records").foreach(println)

  System.in.read()


}
