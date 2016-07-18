package com.weblogchallenge

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration.{Duration, _}

/**
 * Created by asingh on 16-07-14.
 */
object App {

  /******************************* HELPER METHODS ************************************/

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

  /******************************* CONSTANTS ************************************/
  /**
   * Constant defining the [[Duration]] after which the session is considered expired.
   */
  val SESSION_WINDOW = 15 minutes

  /******************************* COMPUTATIONS ************************************/
  /**
   * Extract Ip Address, Timestamp, Request URL from log entry,
   * creates a [[LogLine]] object using these fields.
   * @param lines read by using `sc.textFile()`
   * @return RDD[LogLine]
   */
  def parse(lines: RDD[String]): RDD[LogLine] = lines.map { line =>
    val fields = line.split("\\s+")
    val ipAddr = fields(2).split(":").head
    LogLine(timestamp = parseTimestamp(fields(0)), ipAddress = ipAddr, url = fields(12))
  }

  /**
   * Computes the session by Window
   * Groups the parsed loglines By Ip Address - (IP, Log entries) and
   * Calls [[sessionize]] to compute session_id for each IP Address.
   * @param logLines
   * @param timeout defines to duration for session window, default is [[SESSION_WINDOW]]
   * @return RDD[(String, LogLine)] i.e; (IP, LogLine)
   */
  def sessionizeByWindow(logLines: RDD[LogLine], timeout: Duration): RDD[(String, LogLine)] = {
    val groupedByIp = logLines.groupBy(_.ipAddress)
    groupedByIp.flatMapValues(logs => sessionize(logs.toSeq, timeout))
  }

  /**
   * Computes the Start and End [[Timestamp]] for each user session - (IP, session_id, start_timestamp, end_timestamp)
   * Maps the [[sessionizeByWindow()]] output to Tuple ((IP, session_id), (timestamp, timestamp)) and
   * Groups by (IP, Session_Id) and
   * Reduces by finding the MIN and MAX [[Timestamp]] withing a Session
   * @param sessionized
   * @return RDD[(String, Int, Long, Long)]
   */
  def sessionByTime(sessionized: RDD[(String, LogLine)]): RDD[(String, Int, Long, Long)] = {
    sessionized
      .map(s => ((s._1, s._2.sessionId), (s._2.timestamp, s._2.timestamp))) // duplicate timestamp field for easy computation.
      .reduceByKey { (a, b) =>
        (Math.min(a._1, b._1), Math.max(a._2, b._2))
      }.map(f => (f._1._1, f._1._2, f._2._1, f._2._2)) //flatten the tuple
  }

  /**
   * Computes the Average Session Time by following on the output of [[sessionByTime()]]
   * @param sessByTime
   * @return [[Double]]
   */
  def avgSessionTime(sessByTime: RDD[(String, Int, Long, Long)]): Double = sessByTime.map(a => a._4 - a._3).mean()

  /**
   * Computes the Unique URL's in a Session
   * Maps the [[sessionizeByWindow()]] output to ((IP, session_id), url) and
   * Groups by (IP, Session_Id) and
   * Counts the distinct url's in the seq
   * @param sessionized
   * @return RDD[((String, Int), Int)] -
   *         the last element tuple represents the number of unique urls in the session
   */
  def uniqueUrls(sessionized: RDD[(String, LogLine)]): RDD[((String, Int), Int)] = {
    sessionized
      .map(s => ((s._1, s._2.sessionId), s._2.url))
      .groupByKey
      .mapValues(_.toSeq.distinct.size)
  }

  /**
   * Computes the Most Engaged Visitors
   * Maps the [[sessionByTime]] to (IP, Session_Id, Long : Diff bet end and start timestamp) and
   * Sorts it by time diff in the descending order.
   * @param sessByTime
   * @return  RDD[(String, Int, Long)]
   */
  def mostEngagedVisitors(sessByTime: RDD[(String, Int, Long, Long)]): RDD[(String, Int, Long)] = {
    sessByTime
      .map(s => (s._1, s._2, s._4 - s._3))
      .sortBy(_._3, false)

  }

  def main(args: Array[String]) {
    /**
     * Arguments :
     *  0 -> master
     *  1 -> inputFilePath
     *  2- session timeout window
     */

    // spark context
    val conf = new SparkConf()
      .setAppName("WebLogChallenge")
      .setMaster(args(0))
    val sc = new SparkContext(conf)

    /**
     * Parse the logfile.
     * see here for more details [[http://spark.apache.org/docs/latest/programming-guide.html#external-datasets]]
     */
    val lines = sc.textFile(
      Option(args(1)).getOrElse("file:///Users/asingh/Documents/dev/docker/2015_07_22_mktplace_shop_web_log_sample.log"))

    val logLines = parse(lines)
    val sByWindow = sessionizeByWindow(logLines, Option(args(2)).map(Duration(_)).getOrElse(SESSION_WINDOW))
    val sByTime = sessionByTime(sByWindow)
    val avg = avgSessionTime(sByTime)
    val sByUrl = uniqueUrls(sByWindow)
    val sByEngagement = mostEngagedVisitors(sByTime)

    // The above computed RDD's can be saved into a persistent store like Hive, Hbase, textFiles in HDFS etc.
  }
}
