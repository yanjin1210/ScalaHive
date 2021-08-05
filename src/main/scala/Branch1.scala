import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Branch1 {
  def createBranch(spark:SparkSession, branch:String): Unit = {
    spark.sql(f"Drop table if exists $branch")
    spark.sql(f"Create table if not exists $branch (beverage String)")
    for (suffix<-"abc") {
      spark.sql(f"Insert into table $branch select beverage from branch$suffix where branch = '$branch'")
    }
  }

  def createCount(spark:SparkSession, branch:String): Unit = {
    spark.sql(f"Drop table if exists count_$branch")
    spark.sql(f"Create table if not exists count_$branch (beverage String, count int) row format delimited fields terminated by ','")
    for (suffix<-"abc") {
      spark.sql(f"Insert into table count_$branch " +
        f"select c.beverage, c.count from $branch b join " +
        f"(select beverage, sum(count) count from count$suffix group by beverage) c " +
        f"on b.beverage = c.beverage" )
    }
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val DATALOADED = true
    val spark = SparkSession
      .builder
      .appName("HiveConn")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(sqlText = "Use project1")
    if (!DATALOADED) {
      Branch1.createBranch(spark, "Branch1")
      Branch1.createCount(spark, "Branch1")
    }
    spark.sql("Select sum(count) from count_Branch1").show()
  }
}