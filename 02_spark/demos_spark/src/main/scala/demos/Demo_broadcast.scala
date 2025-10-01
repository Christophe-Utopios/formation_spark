package demos

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Demo_broadcast {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("demo-broadcast").getOrCreate()

    import spark.implicits._

    val countryDf = Seq(
      ("US", "United States"),
      ("FR", "France"),
      ("DE", "Germany"),
      ("JP", "Japan")
    ).toDF("country_code", "country_name")

    val userDf = Seq(
      ("Toto", "US", 30),
      ("Tata", "FR", 20),
      ("Titi", "DE", 24),
      ("Tutu", "FR", 28),
      ("Tutu", "AA", 28),
    ).toDF("name", "country_code", "age")

    // Broadcast join (Spark optimise automatiquement les petites tables)
    val enrichedDf = userDf.join(
      broadcast(countryDf),
      Seq("country_code")
    )

    // Broadcast de variable de config

    case class FilterConfig(
                              minAge: Int,
                              allowedCountries: Set[String]
                           )

    val config = FilterConfig(
      minAge = 25,
      allowedCountries = Set("FR", "US")
    )

    val broadcastConfig = spark.sparkContext.broadcast(config)

    val userFilterUDF = udf((age: Int, country: String) => {
      val conf = broadcastConfig.value
      age >= conf.minAge && conf.allowedCountries.contains(country)
    })

    val filteredUserDF = userDf.filter(userFilterUDF(col("age"), col("country_code")))

    filteredUserDF.show()

    // Création d'une Map
    val countryMapping = Map(
      "US" -> "United States",
      "FR" -> "France",
      "DE" -> "Germany",
      "JP" -> "Japan"
    )

    val broadcastCountries: Broadcast[Map[String,String]] = spark.sparkContext.broadcast(countryMapping)

    val getCountryUDF = udf((code : String) => {
        broadcastCountries.value.getOrElse(code, "Unknown")
    })

    val userWithUdf = userDf.withColumn("country_name", getCountryUDF(col("country_code")))

    userWithUdf.show()
    broadcastCountries.destroy()
    broadcastConfig.destroy()
  }
}
