import org.apache.spark.sql.SparkSession

object CompactFiles {
  val inputPath = "test-out"
  val outputPath = "compact-out"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .getOrCreate()

    import spark.implicits._

    // load all files in the directory
    val serviceStatsIn = spark.read.parquet(inputPath).as[HealthMonitorStats].rdd

    // compact the files
    val serviceStatsOut = serviceStatsIn.map(elem => (
      (elem.service, elem.time), // Key
      ( // Value
        elem.ACpu, elem.PCpu, elem.TCpu,
        elem.ARam, elem.PRam, elem.TRam,
        elem.ADisk, elem.PDisk, elem.TDisk,
        elem.count
      )
    ))
    .reduceByKey((val1, val2) => (
      (val1._1 * val1._10 + val2._1 * val2._10) / (val1._10 + val2._10), // Mean CPU utilization
      if (val1._2 > val2._2) val1._2 else val2._2, // Max CPU utilization
      if (val1._2 > val2._2) val1._3 else val2._3, // Max CPU utilization time
      (val1._4 * val1._10 + val2._4 * val2._10) / (val1._10 + val2._10), // Mean RAM
      if (val1._5 > val2._5) val1._5 else val2._5, // Max RAM
      if (val1._5 > val2._5) val1._6 else val2._6, // Max RAM time
      (val1._7 * val1._10 + val2._7 * val2._10) / (val1._10 + val2._10), // Mean Disk
      if (val1._8 > val2._8) val1._8 else val2._8, // Max Disk
      if (val1._8 > val2._8) val1._9 else val2._9, // Max Disk time
      val1._10 + val2._10 // Count
    ))
    .map(elem => HealthMonitorStats(
      elem._1._1, elem._1._2, elem._2._10,
      elem._2._1, elem._2._2, elem._2._3, // CPU
      elem._2._7, elem._2._8, elem._2._9, // Disk
      elem._2._4, elem._2._5, elem._2._6  // RAM
    ))

    // write the output to folder
    serviceStatsOut.toDF().coalesce(1).write.mode("append").parquet(outputPath)
  }
}
