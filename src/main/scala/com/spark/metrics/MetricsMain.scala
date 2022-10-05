package com.spark.metrics

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.io.StdIn.readLine


object MetricsMain {

  def main(args: Array[String]): Unit = {
    var spark = getSparkSession()
//    spark.sparkContext.addSparkListener(new SparkMetricListener())
    var df  = spark.range(1, 100000000000L).as("id").toDF

    df = df
      .withColumn("random1", round(rand()*(10-5)+5,0))
      .withColumn("random2", round(rand()*(10-5)+5,0))
      .withColumn("random3", round(rand()*(10-5)+5,0))
      .withColumn("random4", round(rand()*(10-5)+5,0))
      .withColumn("random5", round(rand()*(10-5)+5,0))
      .withColumn("random6", round(rand()*(10-5)+5,0))
      .withColumn("random7", round(rand()*(10-5)+5,0))
      .withColumn("random8", round(rand()*(10-5)+5,0))
      .withColumn("random9", round(rand()*(10-5)+5,0))

    df.show
    df.groupBy(spark_partition_id()).count.show

    readLine("Press Enter to end")

    spark.stop()
    spark.close()
  }

  def getSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Metric")
      .config("spark.extraListeners", "com.spark.metrics.SparkMetricListener")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
//    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    spark
  }
}
