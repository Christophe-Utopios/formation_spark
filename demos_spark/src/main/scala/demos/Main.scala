package demos

object Main {
  def main(args: Array[String]): Unit = {
    // 1. Déclaration de variables
    val immutable = "ne change pas"
    var mutable = "peut changer"

    // 2. Collections
    val liste = List(1,2,3,4,5)
    val tableau = Array("a","b","c")
    val map = Map("prenom" -> "Toto", "age" -> 25)

    // 3. Fonctions anonymes (lambdas)
    val addition = (a : Int, b : Int) => a + b

    def maFonction(a: Int, b : Int) : Int = {
      return a + b
    }

    val test = (age : Int) => age > 20
  }
}