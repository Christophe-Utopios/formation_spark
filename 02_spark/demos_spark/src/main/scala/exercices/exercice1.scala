package exercices

import org.apache.spark.sql.SparkSession

object exercice1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("exercice1").getOrCreate()
    val sc = spark.sparkContext

    // Lecture du fichier CSV
    val rddachats = sc.textFile("./Data/achats_clients.csv")

    // Affichage des 10 premières lignes
    rddachats.take(10).foreach(println)

    // Filtrage de la première ligne (header)
    val firstLine = rddachats.first()
    val rdd = rddachats.filter(ligne => ligne != firstLine)
    rdd.take(5).foreach(println)

    // Transformation : extraction id_client et montant
    val rddClients = rdd.map(line => {
      val fields = line.split(",")
      (fields(0), fields(2).toDouble)
    })
    rddClients.take(5).foreach(println)

    // Agrégation par client (somme des montants)
    val montantClients = rddClients.reduceByKey((a, b) => a + b)
    val result = montantClients.collect()

    // Affichage du résultat
    result.foreach(println)

    // Affichage formaté
    result.foreach { case (idClient, montant) =>
      println(f"Client $idClient: $montant%.2f€")
    }
  }
}
