package demos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Demo_df_join {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("demo-df-join").getOrCreate()

    val sc = spark.sparkContext

    import  spark.implicits._

    val livresSchema = StructType(Array(
      StructField("livre_id", StringType, nullable = false),
      StructField("titre", StringType, nullable = false),
      StructField("auteur_id", StringType, nullable = true)
    ))

    val livresData = Seq(
      ("L1", "Titre 1", "A1"),
      ("L2", "Titre 2", "A2"),
      ("L3", "Titre 3", "A1"),
      ("L4", "Titre 4", "A3"),
      ("L5", "Titre 5", null)
    )

    val dfLivres = livresData.toDF("livre_id", "titre", "auteur_id")

    dfLivres.show()

    val auteurSchema = StructType(Array(
      StructField("auteur_id", StringType, nullable = false),
      StructField("nom", StringType, nullable = false)
    ))

    val auteurData = Seq(
      ("A1", "Toto"),
      ("A2", "Titi"),
      ("A3", "Tata")
    )

    val dfAuteurs = auteurData.toDF("auteur_id", "nom")

    // INNER JOIN - Seulement les livres avec un auteur
    val newDf = dfLivres.join(dfAuteurs, Seq("auteur_id") ,"inner")

    newDf.show()

    // LEFT JOIN - Tous les livres, et seulement les auteurs avec un livre
    val newDf2 = dfLivres.join(dfAuteurs, dfLivres("auteur_id") === dfAuteurs("auteur_id") ,"left")

    newDf2.show()

    // ANTI JOIN - Tous les livres sans auteur
    val newDf3 = dfLivres.join(dfAuteurs, Seq("auteur_id") ,"anti")

    newDf3.show()
  }
}
