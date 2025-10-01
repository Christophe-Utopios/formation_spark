package demos

import org.apache.spark.sql.SparkSession
import plotly.Plotly.plot
import plotly._
import plotly.element.HoverInfo.Color
import plotly.element.Marker
import plotly.layout.Layout
import plotly.layout.Axis

object Demo_plotly {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("demo-plotly").getOrCreate()

    import spark.implicits._

    val salesDF = Seq(
      ("Janvier", 150),
      ("Février", 100),
      ("Mars", 50),
      ("Avril", 75),
      ("Mai", 200),
    ).toDF("mois", "ventes")

    // Conversion DataFrame vers données Plotly
    val salesData = salesDF.collect()
    val mois = salesData.map(_.getString(0)).toSeq
    val ventes = salesData.map(_.getInt(1)).toSeq

    // Créer un graphique en barres
    val graph1 = Bar(
      x = mois,
      y = ventes,
      name = "Ventes Mensuelles"
    )

    val layoutGraph1 = Layout(
      title = "Ventes Mensuelles",
      xaxis = Axis(title = "Mois"),
      yaxis = Axis(title = "Ventes")
    )

    plot("ventes_mensuelles.html", Seq(graph1), layoutGraph1)
  }
}
