import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

import java.sql.Timestamp

object StreamProcessing {

  val interval = 20
  val hostName = "localhost"
  val port = 9999
  val outputPath = "test-out"

  def main(args: Array[String]): Unit = {
    // Create a local StreamingContext
    val conf = new SparkConf().setMaster("local[*]").setAppName("HealthMonitoringStreamStats")
    val ssc = new StreamingContext(conf, Seconds(interval))

    // Read the CSV row by row where each line represent a row
    val lines = ssc.socketTextStream(hostName, port)
    val serviceStatsOut = computeStreamOut(lines)

    // Start a spark session to save the output
    val spark = SparkSession
      .builder()
      .appName("HealthMonitoringStreamStats")
      .getOrCreate()

    import spark.implicits._

    serviceStatsOut.foreachRDD(rdd => {
      val df = rdd.toDF()
      if(!df.isEmpty) {
        df.show()
        df.coalesce(1).write.mode("append").parquet(outputPath)
      }
    })

//    ServiceStatsOut.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

  def computeStreamOut(lines: ReceiverInputDStream[String]) : DStream[HealthMonitorStats] =  {
    val splits = lines.map(_.split(","))

    val serviceInfo = splits
      .map(arr => (
        arr(0), // Service name
        arr(1).toLong * 1000, // Timestamp
        arr(2).toDouble, // CPU utilization
        1 - (arr(4).toDouble / arr(3).toDouble), // RAM utilization
        1 - (arr(6).toDouble / arr(5).toDouble) // Disk utilization
      ))
      .map(arr => (
        (arr._1, arr._2 / 1000), // Key (Service name, Timestamp in minutes)
        ( // Value
          arr._3, arr._3, arr._2,
          arr._4, arr._4, arr._2,
          arr._5, arr._5, arr._2,
          1L
        )
      ))

    val serviceStats = serviceInfo.reduceByKey((val1, val2) => (
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

    val serviceStatsOut = serviceStats.map(elem => HealthMonitorStats(
      elem._1._1, new Timestamp(elem._1._2 * 1000), elem._2._10,
      elem._2._1, elem._2._2, new Timestamp(elem._2._3), // CPU
      elem._2._7, elem._2._8, new Timestamp(elem._2._9), // Disk
      elem._2._4, elem._2._5, new Timestamp(elem._2._6)  // RAM
    ))

    serviceStatsOut
  }
}

/*
Input:
Service1,1646485461,0.2,4,1.5,100,75
Service1,1646485462,0.7,4,0.5,100,70
Service1,1646485463,0.3,4,0.5,100,20
Service2,1646485463,0.3,4,0.5,100,20
Service3,1646485463,0.3,4,0.5,100,20
Service4,1646485463,0.3,4,0.5,100,20
Service5,1646485463,0.3,4,0.5,100,20

((Service1,1646485),(0.39999999999999997,0.7,1646485462,0.7916666666666666,0.875,1646485463,0.45,0.8,1646485463,3))
 */