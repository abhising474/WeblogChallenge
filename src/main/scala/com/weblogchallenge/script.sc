/**
 * Copy and paste this script (after line #11 as sc is already provided) using :paste mode in spark-shell,
 * hit Ctrl + D to compile and run the script.
 * NOTE : Update the Input file location before running script
 */

import org.apache.spark.{ SparkConf, SparkContext }

// spark context - only for code completion
val conf = new SparkConf().setAppName("Weblog").setMaster("local")
val sc = new SparkContext(conf)

/**
 * Beginning of script : Computes the following
    Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window. https://en.wikipedia.org/wiki/Session_(web_analytics)

    Determine the average session time

    Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

    Find the most engaged users, ie the IPs with the longest session times
 */

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import java.io.File
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

/**
 * Logline helper case class.
 * Each log entry will be mapped to this class.
 *
 * @param ipAddress
 * @param timestamp
 * @param sessionId
 * @param url
 */
case class LogLine(ipAddress: String, timestamp: Long, sessionId: Int = 1, url: String)

/**
 * Given the input [[Seq[LogLine]] log lines grouped by Ip Address,
 * this function computes the session_id. It compares two consecutive log entries,
 * if the Duration between the timestamps is greater than the [[SESSION_WINDOW]], i.e;
 * the previous session timed out, it creates a new session_id otherwise it uses the existing
 * session_id.
 *
 * @param lines
 * @param timeout
 * @return updated [[Seq[LogLine]] having session_id.
 */
def sessionize(lines: Seq[LogLine], timeout: Duration) = {
  lines.tail.scanLeft(lines.head) { (prev, curr) =>
    if (curr.timestamp - prev.timestamp > timeout.toMillis) curr.copy(sessionId = curr.sessionId + 1)
    else curr.copy(sessionId = prev.sessionId)
  }
}

/**
 * Helper to parse date from the log entries.
 *
 * @param str
 * @return [[LocalDateTime]]
 */
def parseDate(str: String): LocalDateTime = LocalDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME)
def parseTimestamp(str: String): Long = Timestamp.valueOf(parseDate(str)).getTime

/**
 * Constant defining the [[Duration]] after which the session is considered expired.
 */
val SESSION_WINDOW = 15 minutes

/**
 * setup output directory
 */
val outDir = {
  val path = System.getProperty("java.io.tmpdir") + "weblogchallenge" + File.separator
  new File(path).mkdir()
  s"file://$path"
}
println(s"setting out dir to ==> $outDir")

/**
 * Load the input logfile.
 * see here for more details [[http://spark.apache.org/docs/latest/programming-guide.html#external-datasets]]
 */
val lines = sc.textFile("file:///Users/asingh/Documents/dev/docker/2015_07_22_mktplace_shop_web_log_sample.log")

/**
 * Extract Ip Address, Timestamp, Request URL from log entry,
 * creates a [[LogLine]] using these fields.
 */
val loglines = lines.map { line =>
  val fields = line.split("\\s+")
  val ipAddr = fields(2).split(":").head
  LogLine(timestamp = parseTimestamp(fields(0)), ipAddress = ipAddr, url = fields(12))
}

// Grouped By Ip Address - (IP, Log entries)
val groupedByIp = loglines.groupBy(_.ipAddress)

// Call sessionize to compute session_id, using Log Entries for each IP Address.
val sessionized = groupedByIp.flatMapValues(logs => sessionize(logs.toSeq, SESSION_WINDOW))

/**
 * Formats the Sessonized to a more readable format and
 * collects data from all partitions and
 * writes it to the file sessionize-timestamp.
 */
sessionized.map(x => s"${x._2.ipAddress} ${new Timestamp(x._2.timestamp).toLocalDateTime} ${x._2.sessionId} ${x._2.url}")
  .repartition(1).saveAsTextFile(outDir + "sessionize-" + System.currentTimeMillis())

// =========== Average Session Time ============

/**
 * Map the sessionize output to ((IP, session_id), (timestamp, timestamp)) and
 * Group by (IP, Session_Id) and
 * compute the Start and End Timestamp for each user session.
 * (IP, session_id, start_timestamp, end_timestamp)
 */
val sessionByTime = sessionized
  .map(s => ((s._1, s._2.sessionId), (s._2.timestamp, s._2.timestamp))) // duplicate timestamp field for easy computation.
  .reduceByKey { (a, b) =>
    (Math.min(a._1, b._1), Math.max(a._2, b._2))
  }.map(f => (f._1._1, f._1._2, f._2._1, f._2._2))

/**
 * Formats the [[sessionByTime]] to a more readable format and
 * collects data from all partitions and
 * writes it to the file session-by-time-timestamp.
 */
sessionByTime.map(t => s"${t._1} ${t._2} ${t._3} ${t._4}")
  .repartition(1).saveAsTextFile(outDir + "session-by-time-" + System.currentTimeMillis())

// difference between end and start timestamps and calculate mean
val avgSessionTime = sessionByTime.map(a => a._4 - a._3).mean()

/**
 * Converts the [[avgSessionTime]] from Double to RDD[Double] and
 * Formats the [[avgSessionTime]] to a more readable format and
 * collects data from all partitions and
 * writes it to the file avg-session-timestamp.
 */
sc.parallelize(Seq(avgSessionTime).map(a => s"avg session time is : ${Duration(a, TimeUnit.MILLISECONDS).toMinutes} Minutes"))
  .repartition(1).saveAsTextFile(outDir + "avg-session-" + System.currentTimeMillis())

// =========== Unique URL's per session ============
/**
 * Map the sessionize output to ((IP, session_id), url) and
 * Group by (IP, Session_Id) and
 * count the distinct url's in the seq
 */
val sessionByUrl = sessionized
  .map(s => ((s._1, s._2.sessionId), s._2.url))
  .groupByKey
  .mapValues(_.toSeq.distinct.size)

/**
 * Formats the [[sessionByUrl]] to a more readable format and
 * collects data from all partitions and
 * writes it to the file session-by-url-timestamp.
 */
sessionByUrl.map(u => s"${u._1._1} ${u._1._2} ${u._2}")
  .repartition(1).saveAsTextFile(outDir + "session-by-url-" + System.currentTimeMillis())

// =========== Most Engaged Visitors ============

/**
 * Maps the [[sessionByTime]] to (IP, Session_Id, Long : Diff bet end and start timestamp) and
 * sorts it by time diff in the descending order.
 */
val sortedBySession = sessionByTime
  .map(s => (s._1, s._2, s._4 - s._3))
  .sortBy(_._3, false)

/**
 * Formats the [[sortedBySession]] to a more readable format and
 * collects data from all partitions and
 * writes it to the file session-sorted-by-time-timestamp.
 */
sortedBySession.map(s => s"${s._1} ${s._2} ${s._3} ${Duration(s._3, TimeUnit.MILLISECONDS).toMinutes} Minutes")
  .repartition(1).saveAsTextFile(outDir + "session-sorted-by-time-" + System.currentTimeMillis())

/**
 * End of Script
 */
