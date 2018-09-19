import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
object task2 {
  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val df1 = spark.read.format("json").json(args(0))
    val df2 = spark.read.format("json").json(args(1))
    val joined_df = df1.join(df2
      , df1.col("asin").equalTo(df2.col("asin")))
    val df = joined_df.select("brand","overall")
    val out_df = df.where(df.col("brand").isNotNull)
    val final_df = out_df.filter("brand != ''")
    //val new_final_df = final_df.groupBy("brand").agg(count("brand"),sum("overall"),avg("overall")).sort("brand").show()
    val new_final_df = final_df.groupBy("brand").agg(avg("overall").alias("rating_avg")).sort("brand")
    val persistedData = new_final_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    // force evaluation
    persistedData.count
    // write data
    new_final_df.coalesce(1).write.option("header", "true").csv(args(2))
  }
}

