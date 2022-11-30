import org.apache.arrow.flatbuf.Date
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{CurrentTimestamp, Year}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, StringType, StructType}


object BadgesTenureReputation {


  def main(array: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", "session demo")
    sparkConf.set("spark.master", "local[2]")
    val sc = new SparkContext(sparkConf)

    val spark = SparkSession.builder()
      .config(sparkConf)
      //.enableHiveSupport()
      .getOrCreate()

    val badges = spark.read
      .format("xml")
      .option("rootTag", "badges")
      .option("rowTag", "row")
      .option("inferSchema", "true")
      .option("path", array(0))
      .load()
      .select("_Id","_UserId")
      .groupBy("_UserId").agg(count("_UserId").as("NumberOfBadges"))

    badges.printSchema()
    badges.show()


    val users = spark.read
      .format("xml")
      .option("rootTag", "users")
      .option("rowTag", "row")
      .option("inferSchema", "true")
      .option("path", array(1))
      .load()
      .select("_Id", "_Reputation","_CreationDate")
      .withColumn("Tenure",round(datediff(current_timestamp(),col("_CreationDate"))/365.25,0))

    users.printSchema()
    users.show()

    badges.createOrReplaceTempView("b")
    users.createOrReplaceTempView("u")

    val x = spark.sql("Select b._UserId,b.NumberOfBadges,u._Reputation,u.Tenure from b join u on b._UserId = u._Id ")
    x.show()

    val y = x.groupBy("Tenure").agg(count("_UserId").as("NumOfUsers"),round(avg("NumberOfBadges"),1).as("AvgBadge"),round(avg("_Reputation"),1).as("AvgReputation"))
    y.show()

    y.write
      .mode("overwrite")
      .format("csv")
      .save(array(2))

  }
}