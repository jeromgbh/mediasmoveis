import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jero on 19/11/15.
  * Spark
  */
object MediasMoveis {
  def main(args: Array[String]): Unit = {

    println("Iniciando")

    // Help
    if (args.length > 1 && args(0) == "--help") {
      System.err.println("Usage: macd <dir> <master> <workers>\n")
      System.exit(1)
    }

    // Argumentos e dados default
    val dir = if (args.length > 0 && args(0) != null) args(0) else "hdfs://bryce.i2.com.br/bovespa/expand/*201508*"
    val url = if (args.length > 1 && args(1) != null) args(1) else "bryce.i2.com.br"

    // Se master for local, URL e' apenas local
    val master = if (url != "local") "spark://" + url + ":7077"

    // Configura e instancia o Spark
    println("Instanciando Spark com master = " + master)
    val conf = new SparkConf()
    conf.setAppName("Medias Moveis Bovespa")
    conf.setMaster(master.toString)
    val sc = new SparkContext(conf)

    println("Massa de dados = " + dir)
    // Sem espacos, cabecalhos e roda-pes
    val bovespa = sc.textFile(dir).
      filter(line => line.contains(';')). // apenas transacoes
      map(elem => elem.replaceAll("\\s", "")) // remove espacos

    // Calcula as medias diarias
    val mediasDiarias = bovespa.map(_.split(";")). // separa campos
      map(itens => (itens(1) + ":" + itens(0), (itens(3).toFloat, 1f))). // chave composta c/ valor da transacao
      reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)). //acumula valores e quantidades
      mapValues(v => v._1 / v._2). // calcula media
      cache() // coloca no cahce porque sera' utilizado novamente

    mediasDiarias.take(10).foreach(println)

    val qtde0 = mediasDiarias.count()

    println("Quantidade => "+qtde0)

    val simbolos = mediasDiarias.keys.map(_.split(":")). // seleciona as chaves e divide o campo composto
      map(simbol => simbol(0)). // separa o codigo da acao
      distinct(). // elimina repetidos
      cache() // coloca no cahce porque sera' utilizado novamente

    simbolos.take(10).foreach(println)

    val qtde = simbolos.count()

    simbolos.collect().foreach( s => {
      println("Simbolo => "+s)
      mediasDiarias.filter(reg => reg._1.startsWith(s)).sortByKey().take(10).foreach(println)
    })

    println("Quantidade => "+qtde)



  }

}
