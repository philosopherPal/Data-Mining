import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.jackson.JsonMethods._
import java.io._
object task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("task1Rdd")
      .setSparkHome("src/main/resources")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(args(0))
    val newRDD = rdd.map(jsonstring => {
      implicit val formats = DefaultFormats
      val parsedJson = parse(jsonstring)
      val key = (parsedJson \ "asin").extract[String]
      val val_ue = (parsedJson \ "overall").extract[String]
      (key,val_ue)
    })
    val avg_value = newRDD.mapValues(overall => (overall.toFloat, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).sortByKey()
    val avg_rating = avg_value.mapValues{ case (sum, count) => sum / count }
    val pw = new PrintWriter(new File(args(1)))
    pw.write("asin,rating_avg\r\n")

    val arr: Array[(String, Float)] = avg_rating.collect()
    for (i <- 0 to arr.length - 1) {
      pw.write(arr(i)._1 + "," + arr(i)._2 + "\r\n")
    }
    pw.close
  }
}
