import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Which day of the week has most questions answered within an hour?
object MostAnsweredDay {
  def main(args:Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .appName("MostAnsweredDay")
      .master("local[*]")
      .getOrCreate();

    val data = spark.read
      .format("xml")
      .option("rootTag","posts")
      .option("rowTag","row")
      .option("inferSchema",true)
      .option("path",args(0))
      .load()
      .select("_Id","_ParentId","_PostTypeId","_CreationDate")

    val questions = data
      .filter(col("_PostTypeId")===1)
      .select("_Id","_CreationDate")

    val answers = data
      .filter(col("_PostTypeId")===2)
      .select("_Id","_CreationDate","_ParentId")

    val window = Window.partitionBy("_ParentId").orderBy("_CreationDate")

    val first_answer = answers.withColumn("row_number",row_number().over(window))
    // first_answer.show(5);

    questions.createOrReplaceTempView("q_t")
    first_answer.filter(col("row_number")===1).createOrReplaceTempView("a_t")

    val Question_Answer = spark
      .sql("select q_t._Id as id,q_t._CreationDate as ques_date,a_t._CreationDate as ans_date from q_t join a_t on q_t._Id = a_t._ParentId")

    // Question_Answer.show(10);

    val time_for_answer = Question_Answer
      .withColumn("time_in_minutes",round(((col("ans_date").cast("Long")-col("ques_date").cast("Long"))/86400)*24*60,1));

    //time_for_answer.show(20);

    val less_than_hour = time_for_answer.filter(col("time_in_minutes")<=60.0);
    //less_than_hour.show(10);

    val question_day = less_than_hour
      .withColumn("question_posted_day",dayofweek((col("ques_date"))))

    //question_day.show(10);

    val count_of_answered = question_day.groupBy("question_posted_day")
      .agg(count("id").as("Total_Count"))
      .withColumn("Day",
            when(col("question_posted_day")===1,"Sunday")
           .when(col("question_posted_day")===2,"Monday")
           .when(col("question_posted_day")===3,"Tuesday")
           .when(col("question_posted_day")===4,"Wednesday")
           .when(col("question_posted_day")===5,"Thursday")
           .when(col("question_posted_day")===6,"Friday")
           .when(col("question_posted_day")===7,"Saturday"))
      .select("Day","Total_Count")

    count_of_answered.write.mode("overwrite").format("csv").save(args(1))



  }
}
