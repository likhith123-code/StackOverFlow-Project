import org.apache.arrow.flatbuf.Date
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object PercentageQuestionsAnswered {
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
    val df_posts = spark.read
      .format("xml")
      .option("rootTag", "posts")
      .option("rowTag", "row")
      .option("inferSchema", true)
      .option("path", array(0))
      .load()
      .select("_Id","_PostTypeId","_AnswerCount","_CreationDate")
      .filter(col("_PostTypeId")===1)

    df_posts.printSchema()
    df_posts.show(10)
    val df_year = df_posts.withColumn("Year",date_format(col("_CreationDate"), "y"))
    df_year.show(10)


    val df= df_year.withColumn("AnsweredOrNot",when(col("_AnswerCount") >0,1).otherwise(0))
    val df_final=df.groupBy("Year").agg(count("_Id").as("NumberOfQuestions"),sum("AnsweredOrNot").as("TotalSum"))
      .withColumn("Percentage",round(col("TotalSum")/col("NumberOfQuestions")*100,2))
    df_final.write.format("csv").mode("overwrite").save(array(1));
    df_final.show();

  }
}
