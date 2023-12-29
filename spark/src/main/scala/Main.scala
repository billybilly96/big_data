import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import java.text.DecimalFormat

object Main extends App {

  // Function to parse the accidents records and returns the useful columns
  def extract(row: String): (String, Double) = {
    val columns = row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
    // Get the needed columns and return them
    val severity = columns(3).toDouble
    val year = columns(4).substring(0, 4)
    val city = columns(15)
    (year + city, severity)
  }

  override def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("BDE Spark Ragazzi").getOrCreate()
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val inputPath = "hdfs:/user/aravaglia/exam_lr/accidents.csv"
    val outputPath = new Path("hdfs:/user/aravaglia/exam_lr/spark/accidents")
    // Check if output path has already present to delete it
    if (fs.exists(outputPath))
      fs.delete(outputPath, true)

    val lastYear = "2019"
    val secondLastYear = "2018"
    val topCities = 100
    val df = new DecimalFormat("#.####")

    // Create an RDD from the files in the input path and coalesce
    val rddAccidents = spark.sparkContext.textFile(inputPath).coalesce(10)
    // Extract the needed columns, create key-value pairs and filter records about the last 2 years
    val rddAccidentsKv = rddAccidents.map(x => extract(x)).filter(x => x._1.substring(0, 4) == lastYear || x._1.substring(0, 4) == secondLastYear)
    // Aggregate by key to compute the sum and the count of severity values
    val rddSeverityDataPerCityYear = rddAccidentsKv.aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
    // Compute the average severity in each record
    val rddAvgSeverityPerCityYear = rddSeverityDataPerCityYear.map({case(k, v) => (k, v._1/v._2)})
    // Sort and group by city
    val rddAvgSeverityPerCity = rddAvgSeverityPerCityYear.sortByKey(false).map(x => (x._1.substring(4), x._2)).groupByKey()
    // Filter only the city having values for both last two years and compute the difference
    val rddDifferenceAvgSeverityBetweenLast2Years = rddAvgSeverityPerCity.filter({case(_, values) => values.toList.length == 2}).map(x => (x._1, x._2.toList.foldLeft(x._2.toList.head * 2)(_ - _)))
    // Sort descending the severity value and ascending the city and take the top N (topCities) records
    val rddResult = rddDifferenceAvgSeverityBetweenLast2Years.sortBy({case(k, v) => (-v, k)}).zipWithIndex().filter(_._2 < topCities).map(x => (x._1._1, df.format(x._1._2)))
    // Coalesce and save the RDD on HDFS
    rddResult.coalesce(1).saveAsTextFile(outputPath.toString)
  }
}