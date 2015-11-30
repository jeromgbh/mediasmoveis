import org.apache.spark.SparkContext

/**
  * Created by jero on 30/11/15.
  * Teste Spark
  */
class EMA (private val sc: SparkContext,
           private val simbolo: String,
           private val periodo: Int,
           private val timeSerie : List[String])
  extends Serializable {

  def ema0 (indice: List[String]) {
    if (timeSerie.length < periodo) {
      return 0
    } else {
      val total: Float = 0
      println(total)
    }
  }

}
