package demos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Demo_df {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("demo-df").getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv("./Data/friends-with-header.csv")

    df.show()

    // Select
    df.select("name").show()
    df.select("name", "age").show()

    // filter
    df.filter((col("age") > 35) && (col("friendsNumber") > 300))
      .show()

    val listePrenoms = List("Jean-Luc", "Hugh")

    df.filter(col("name").isin(listePrenoms: _*)).sort("name").show()

    val dfWithBirthYear = df.withColumn("BirthYear", lit(java.time.LocalDate.now().getYear) - col("age"))

    dfWithBirthYear.show()

    // Agrégations
    val dfAge = df.select(
      min("age").alias("age_min"),
      max("age").alias("age_max"),
      avg("age").alias("age_avg")
    ).collect()

    println(dfAge.toList)

//    val result = spark.sql("SELECT * FROM ")
  }
}
