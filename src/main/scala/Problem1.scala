import org.apache.spark.sql.SparkSession

object Problem1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("HiveConn")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("Use project1")
    val res1DF = spark.sql("select sum(count) from count_Branch1").toDF()
    val res2DF = spark.sql("select sum(count) from count_Branch2").toDF()
    println("Problem 1:\nTotal costumers for branch1:")
    res1DF.show()
    println("Total costumers for branch2:")
    res2DF.show()
  }
}
