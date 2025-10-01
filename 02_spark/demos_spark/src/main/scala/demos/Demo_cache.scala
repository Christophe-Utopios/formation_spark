package demos

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage
import org.apache.spark.storage.StorageLevel

object Demo_cache {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("demo-cache").getOrCreate()

    // Différents niveaux de stockage
    val storageLevels = Map(
      // Stockage uniquement dans la mémoire RAM
      // Avantage : Accès plus rapide
      // Inconvénients : Risque de perte si pas assez de RAM, consomme beaucoup de mémoire
      // Usage : Datasets petits/moyens seulement si RAM suffisante
      "MEMORY_ONLY" -> StorageLevel.MEMORY_ONLY,
      // Stocke en mémoire en priorité, utilise le disque si RAM insuffisante
      // Avantage : Plus sécurisé (pas de perte), bon compromis performance/sécurité
      // Inconvénients : plus lent que memory_only
      // Usage : Cas général recommandé pur la plupart des cas
      "MEMORY_AND_DISK" -> StorageLevel.MEMORY_AND_DISK,
      // Stockage uniquement dans la mémoire RAM sous forme compressée
      // Avantage : Economise de la RAM (10x moins)
      // Inconvénients : Risque de perte lors de la compression.décompression
      // Usage : Datasets volumineux
      "MEMORY_ONLY_SER" -> StorageLevel.MEMORY_ONLY_SER,
      // Stockage uniquement sur le disque
      // Avantage : N'utilise pas de ram, peut gérer des datasets volumineux
      // Inconvénients : Très lent
      // Usage : RAM très limitée
      "DISK_ONLY" -> StorageLevel.DISK_ONLY,
      // Stockage uniquement dans la mémoire RAM avec réplication sur 2 noeuds
      // Avantage : Tolérance aux pannes
      // Inconvénients : Double consommation RAM
      // Usage : Datasets sensibles
      "MEMORY_ONLY_2" -> StorageLevel.MEMORY_ONLY_2
    )

    val numbersRDD: RDD[Int] = spark.sparkContext.parallelize(1  to 10000000)

    val expensiveRDD = numbersRDD.filter(_ % 2 == 0).map(x => x * x).filter(_ > 1000)

    println("sans cache :")
    val start1 = System.currentTimeMillis()
    val count1 = expensiveRDD.count()
    val sum1 = expensiveRDD.sum()
    val end1 = System.currentTimeMillis()
    println(s"Temps sans cache : ${end1 - start1}ms")

    println("avec cache :")
    expensiveRDD.cache() // MEMORY_ONLY
//    expensiveRDD.persist(StorageLevel.MEMORY_ONLY_SER)

    val start2 = System.currentTimeMillis()
    val count2 = expensiveRDD.count()
    val sum2 = expensiveRDD.sum()
    val end2 = System.currentTimeMillis()
    println(s"Temps avec cache : ${end2 - start2}ms")

    // gestion du cache
    spark.catalog.clearCache() // Supprime les caches
    expensiveRDD.unpersist() // Supprime du cache
  }
}
