import org.apache.spark.sql.SparkSession

object Problem2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("HiveConn")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("Use project1")

    val res1DF = spark.sql("Select beverage, sum(count) total_count from count_branch1 " +
      "group by beverage " +
      "order by total_count DESC " +
      "limit 1").toDF()
    val res2DF = spark.sql("Select beverage, sum(count) total_count from count_branch2 " +
      "group by beverage " +
      "order by total_count " +
      "limit 1").toDF()

    val res3DF = spark.sql("Select round(sum(count)/count(Distinct beverage), 0) avg_count from count_branch2").toDF()

    println("Problem2:")
    println("Most consumed beverage in Branch1:")
    res1DF.show()
    println("Least consumed beverage in Branch2:")
    res2DF.show()
    println("Average beverage consumption in Branch2:")
    res3DF.show()
  }
}
