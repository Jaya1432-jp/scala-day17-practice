import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Day17 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Scala Spark Day 17 - Window Functions")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("===== DAY 17 - WINDOW FUNCTIONS =====")

    val students = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("students.csv")

    println("\n--- Student Data ---")
    students.show()

    val courseWindow = Window
      .partitionBy("course")
      .orderBy(desc("score"))

    println("\n--- ROW_NUMBER ---")
    students
      .withColumn("row_number", row_number().over(courseWindow))
      .show()

    println("\n--- RANK ---")
    students
      .withColumn("rank", rank().over(courseWindow))
      .show()

    println("\n--- DENSE_RANK ---")
    students
      .withColumn("dense_rank", dense_rank().over(courseWindow))
      .show()

    println("\n--- TOP 3 STUDENTS PER COURSE ---")
    students
      .withColumn("rank", dense_rank().over(courseWindow))
      .filter(col("rank") <= 3)
      .show()

    val customerWindow = Window
      .partitionBy("student")
      .orderBy("exam_date")

    println("\n--- LAG AND LEAD ---")
    students
      .withColumn("previous_score", lag("score", 1).over(customerWindow))
      .withColumn("next_score", lead("score", 1).over(customerWindow))
      .orderBy("student", "exam_date")
      .show()

    println("\n--- LATEST RECORD PER STUDENT ---")
    students
      .withColumn("latest_rank",
        row_number().over(
          Window.partitionBy("student").orderBy(desc("exam_date"))
        )
      )
      .filter(col("latest_rank") === 1)
      .show()

    println("\n===== CONCEPTS COMPLETED =====")
    println("row_number, rank, dense_rank")
    println("partitionBy and orderBy")
    println("Top 3 students per course")
    println("lag and lead")
    println("Latest record per student")

    spark.stop()
  }
}
