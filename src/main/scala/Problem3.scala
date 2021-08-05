import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.AnalysisException

object Problem3 {

  def showBev(branch: String, spark:SparkSession): Unit = {
    println(f"Beverage available in $branch:")
    try {
      val bevDF = spark.sql(f"Select distinct beverage from $branch order by beverage")

      println("There are total " + bevDF.count() + f" beverages available in $branch")
      bevDF.show()
    }
    catch {
      case ex: AnalysisException => println(f"$branch not found.\n")
    }
  }

  def showCommonBev(branch1: String, branch2: String, spark: SparkSession): Unit = {
    println(f"Common beverages in $branch1 and $branch2")
    try {
      val commonBevDF = spark.sql(
        f"Select b1.beverage from " +
                f"((select distinct beverage from $branch1) b1 " +
                f"join (select distinct beverage from $branch2) b2 " +
                f"on b1.beverage = b2.beverage)")
      println("There are total " + commonBevDF.count() + f" common beverages in $branch1 and $branch2")
      commonBevDF.show()
    }
    catch {
      case ex: AnalysisException => println(f"No common beverage in $branch1 and $branch2.")
    }
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("HiveConn")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("Use project1")

    println("Problem 3:")
    showBev("Branch1", spark)
    showBev("Branch8", spark)
    showBev("Branch10", spark)
    showCommonBev("Branch4", "Branch7", spark)
  }
}