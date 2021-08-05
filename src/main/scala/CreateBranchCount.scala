import org.apache.spark.sql.SparkSession

object CreateBranchCount {

  def createBranch(spark: SparkSession, branch: String): Unit = {
    println(f"Creating $branch table ...")
    spark.sql(f"Drop table if exists $branch")
    spark.sql(f"Create table if not exists $branch (beverage String)")
    println("Table created. Loading data ...")
    for (suffix <- "abc") {
      spark.sql(f"Insert into table $branch select beverage from branch$suffix where branch = '$branch'")
    }
    val rowCount = spark.sql(f"Select count(beverage) from $branch").first().get(0)
    println(f"$rowCount rows loaded.\n")
  }

  def createCount(spark: SparkSession, branch: String): Unit = {
    println(f"Creating count_$branch table ...")
    spark.sql(f"Drop table if exists count_$branch")
    spark.sql(f"Create table if not exists count_$branch (beverage String, count int) row format delimited fields terminated by ','")
    println(f"Table created. Loading data...")
    for (suffix <- "abc") {
      spark.sql(f"Insert into table count_$branch " +
        f"select c.beverage, c.count from $branch b join " +
        f"(select beverage, sum(count) count from count$suffix group by beverage) c " +
        f"on b.beverage = c.beverage")
    }
    val rowCount = spark.sql(f"Select count(beverage) from count_$branch").first().get(0)
    println(f"$rowCount rows loaded.\n")
  }

  def getUniqueBranchs(spark: SparkSession): Array[String] = {
    val branchesDF = spark.sql("select * from (select distinct branch from brancha as a union select distinct branch from branchb as b union select distinct branch from branchc as c)").toDF()
    branchesDF.show()
    branchesDF.select("branch").rdd.map(r => r(0).toString).collect()
  }

  def main(args: Array[String]): Unit = {
    var DATA_LOADED = true
    val spark = SparkSession
      .builder
      .appName("HiveConn")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(sqlText = "Use project1")
    if (!DATA_LOADED) {
      for (branch <- CreateBranchCount.getUniqueBranchs(spark)) {
        CreateBranchCount.createBranch(spark, branch)
        CreateBranchCount.createCount(spark, branch)
      }
      println("Branch and count_branch tables created.")
      DATA_LOADED = true
    }
    if (DATA_LOADED)
    println("The first 5 records of beverage in Branch1:")
    spark.sql("Select * from branch1 limit 5").toDF().show()
    println("The first 5 records of beverage count in Branch1:")
    spark.sql("Select * from count_branch1 limit 5").toDF().show()
  }
}
