import org.apache.arrow.flatbuf.Date
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object EasierGoldBadges {
  def main(array: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var sparkConf = new SparkConf()
    sparkConf.setAppName("First App")
    sparkConf.setMaster("local[2]")

    var spark = SparkSession.builder()
      .config(sparkConf)
      //.enableHiveSupport()
      .getOrCreate()
    System.out.println(array(0))
    //READ
    val df_badges = spark.read
      .format("xml")
      .option("rootTag", "badges")
      .option("rowTag", "row")
      .option("inferSchema", true)
      .option("path", array(0))
      .load()
      .select("_Id", "_Name", "_UserId", "_Date")
      .filter(col("_Class") === 1)
    df_badges.printSchema()
    df_badges.show(10)
    val df_users = spark.read
      .format("xml")
      .option("rootTag", "users")
      .option("rowTag", "row")
      .option("inferSchema", "true")
      .option("path", array(1))
      .load()
      .select("_Id", "_CreationDate")
    df_users.printSchema()
    df_users.show(10)


    df_badges.createOrReplaceTempView("b")
    df_users.createOrReplaceTempView("u")
    val join = spark.sql("select * from b join u on b._UserId = u._Id order by b._UserId ")
    join.show(10)
    val tenure = join.withColumn("Tenure", round((col("_Date").cast("Long") - col("_CreationDate").cast("Long")) / 86400))
    tenure.show(10);
    var window = Window.partitionBy("_UserId").orderBy(("Tenure"))
    val firstBadge = tenure.withColumn("Rank",row_number().over(window))
      .filter(col("Rank")===1)
    firstBadge.show(10);
    val df_final=firstBadge.groupBy("_Name").agg(count("_Name").as("NumberOfUsers"),avg("tenure").as("AverageTenure")).orderBy(col("NumberOfUsers").desc).limit(10)
    df_final.show()
    df_final.write.format("csv").mode("overwrite").option("header",true).save(array(2));

  }
}