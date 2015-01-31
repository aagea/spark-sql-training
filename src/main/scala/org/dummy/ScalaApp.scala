package org.dummy

import org.apache.spark.SparkContext



object ScalaApp extends App{
  val logFile = "person.csv"
  val sc = new SparkContext("local", "Simple", "$SPARK_HOME"
    , List("target/spark-sql-training-1.0.jar"))
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import sqlContext.createSchemaRDD

  case class Person(name: String, age: Int, genre:String)

  val people = sc.textFile(logFile).map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt,p(2)))

  people.registerTempTable("people")

  val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

  teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

}
