package demos

import org.apache.spark.sql.SparkSession

object Demo_RDD_bases {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("demo-rdd").getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("WARN")

    val firstRDD = sc.parallelize(Array(1,2,3))

    val secondRDD = firstRDD.map(e => e * 5)

    val result = secondRDD.collect()

    println("Résultat : ")
    println(result.toList)
  }
}
