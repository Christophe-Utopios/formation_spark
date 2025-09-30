package demos

import org.apache.spark.sql.SparkSession

object Demo_rdd_films {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("demo-film").getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("WARN")

    // Lecture du fichier film.data
    val rddFilm = sc.textFile("./Data/film.data")

    // Extraction de la valeur qui nous intéresse (3éme colonne)
    // 196	242	3	881250949
    // 196,242,3,881250949
    val rddNotes = rddFilm.map(line => line.split("\t")(2))

    // Affichage du type RDD
    println(rddNotes)

    // On compte le nombre d’éléments par valeur.
    val result = rddNotes.countByValue()

    // tri et affichage du resultat
    result.toSeq.sortBy(_._1).foreach {
      case (key, value) =>
        println(s"$key : $value")
    }
    scala.io.StdIn.readLine("Enter pour quitter l'application")
  }
}
