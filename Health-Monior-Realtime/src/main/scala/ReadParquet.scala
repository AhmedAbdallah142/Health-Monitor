import org.apache.spark.sql.SparkSession

object ReadParquet {
  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL basic example")
      .getOrCreate()

    val df = spark.read
      .parquet("test-out/part-00000-3143b9d1-18d4-4566-b524-f60e15cfee00-c000.snappy.parquet")
    df.show()
  }
}
