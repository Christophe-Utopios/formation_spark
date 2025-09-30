package demos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Demo_udf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("demo-udf").getOrCreate()

    import  spark.implicits._

    val data = Seq(
      ("Toto", 25, "Ingénieur", 50000.0),
      ("Tata", 30, "Manager", 75000.0),
      ("Titi", 35, "Développeur", 40000.0)
    )

    val df = data.toDF("nom", "age", "poste", "salaire")

    df.show()

    val categorieAge = udf((age : Int) => {
      age match {
        case age if age < 30 => "Junior"
        case age if age < 40 => "Expérimenté"
        case _ => "Senior"
      }
    })

    val salaireBonus = udf((salaire: Double) => salaire * 1.10)

    val dfWithUdf = df
      .withColumn("categorie_age", categorieAge(col("age")))
      .withColumn("salaire_avec_bonus", round(salaireBonus(col("salaire")), 2))

    dfWithUdf.show()

  }
}
