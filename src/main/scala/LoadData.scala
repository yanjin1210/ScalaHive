
import org.apache.spark.sql.SparkSession


object LoadData {
  val DATA_SOURCE_PREFIX = "file:/C:/data/"

  def createBranchTable(spark:SparkSession, dataSource: String, tableName:String): Unit = {
    println(f"Loading $dataSource ...")
    spark.sql(f"Drop table if exists $tableName")
    spark.sql(sqlText = f"Create table If not exists $tableName (Beverage String, Branch String) row format delimited fields terminated by ','")
    spark.sql(sqlText = f"Load Data Local inpath '$dataSource' Overwrite Into Table $tableName")
    val rowCount = spark.sql(f"Select count(beverage) from $tableName").first().get(0)
    println(f"$rowCount rows loaded.\n")
  }

  def createBranchCount(spark:SparkSession, dataSource: String, tableName: String): Unit = {
    println(f"Loading $dataSource ...")
    spark.sql(f"Drop table if exists $tableName")
    spark.sql(sqlText = f"Create table If not exists $tableName (Beverage String, Count int) row format delimited fields terminated by ','")
    spark.sql(sqlText = f"Load Data Local inpath '$dataSource' Overwrite Into Table $tableName")
    val rowCount = spark.sql(f"Select count(beverage) from $tableName").first().get(0)
    println(f"$rowCount rows loaded.\n")
  }

  def main(args: Array[String]): Unit = {
    println("Loading dataset into database Project1 ...")
    val spark = SparkSession
      .builder
      .appName("HiveConn")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(sqlText = "Create database if not exists project1")
    spark.sql(sqlText = "Use project1")
    for (suffix<-"abc") {
      createBranchTable(spark, DATA_SOURCE_PREFIX + f"bev_branch$suffix.txt", f"branch$suffix")
      createBranchCount(spark, DATA_SOURCE_PREFIX + f"bev_count$suffix.txt", f"count$suffix")
    }
    println("Dataset loaded.")
  }
}
