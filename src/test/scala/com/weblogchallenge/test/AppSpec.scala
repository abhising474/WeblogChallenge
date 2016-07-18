package com.weblogchallenge.test

import java.sql.Timestamp
import java.time.{ ZoneId, LocalDateTime }

import com.weblogchallenge.App
import com.weblogchallenge.App.LogLine
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }

/**
 * Created by asingh on 16-07-17.
 */
class AppSpec extends FlatSpec with Matchers with BeforeAndAfter {

  var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("webLogChallengeTest")

    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  def fixture = new {
    val log = "2015-05-13T23:39:43.945958Z my-loadbalancer 192.168.131.39:2817 10.0.0.1:80 0.000073 0.001048 0.000057 200 200 0 29 \"GET http://www.example.com:80/ HTTP/1.1\" \"curl/7.38.0\" - -"
    val timeSeed = LocalDateTime.now().toLocalDate.atStartOfDay(ZoneId.systemDefault()).toLocalDateTime
    val seed = IndexedSeq(
      LogLine(ipAddress = "192.168.1.10", timestamp = Timestamp.valueOf(timeSeed).getTime, url = "http://www.example.com:80/action1"),
      LogLine(ipAddress = "192.168.1.10", timestamp = Timestamp.valueOf(timeSeed.plusMinutes(1)).getTime, url = "http://www.example.com:80/action2"),
      LogLine(ipAddress = "192.168.1.10", timestamp = Timestamp.valueOf(timeSeed.plusMinutes(5)).getTime, url = "http://www.example.com:80/action2"),
      LogLine(ipAddress = "10.17.123.21", timestamp = Timestamp.valueOf(timeSeed).getTime, url = "http://www.example.com:80/abcd"),
      LogLine(ipAddress = "10.17.123.21", timestamp = Timestamp.valueOf(timeSeed.plusMinutes(20)).getTime, url = "http://www.example.com:80/efgh"),
      LogLine(ipAddress = "10.17.123.21", timestamp = Timestamp.valueOf(timeSeed.plusMinutes(25)).getTime, url = "http://www.example.com:80/efgh"))
  }

  "App" should
    "parse the log" in {
      val logLine = App.parse(sc.parallelize(Seq(fixture.log))).collect().head
      logLine.ipAddress should be("192.168.131.39")
      logLine.timestamp should be(App.parseTimestamp("2015-05-13T23:39:43.945958Z"))
      logLine.url should be("http://www.example.com:80/")
    }

  it should "compute session by window" in {
    val sessionized = App.sessionizeByWindow(sc.parallelize(fixture.seed), App.SESSION_WINDOW)
    val sessions = sessionized.values.collect().map(s => (s.ipAddress, s.sessionId))
    sessions should be(Seq(
      ("192.168.1.10", 1),
      ("192.168.1.10", 1),
      ("192.168.1.10", 1),
      ("10.17.123.21", 1),
      ("10.17.123.21", 2),
      ("10.17.123.21", 2)))
  }

  it should "compute session by time" in {
    val sessionized = App.sessionizeByWindow(sc.parallelize(fixture.seed), App.SESSION_WINDOW)
    val sessionByTime = App.sessionByTime(sessionized)
    val sample = fixture.seed
    sessionByTime.collect().foreach {
      case t @ ("192.168.1.10", 1, s, e) => t should be((sample.head.ipAddress, 1, sample.head.timestamp, sample(2).timestamp))
      case t @ ("10.17.123.21", 1, s, e) => t should be((sample(3).ipAddress, 1, sample(3).timestamp, sample(3).timestamp))
      case t @ ("10.17.123.21", 2, s, e) => t should be((sample(4).ipAddress, 2, sample(4).timestamp, sample(5).timestamp))
    }
  }

  it should "compute avg session time" in {
    val sessionized = App.sessionizeByWindow(sc.parallelize(fixture.seed), App.SESSION_WINDOW)
    val sessionByTime = App.sessionByTime(sessionized)
    val avg = App.avgSessionTime(sessionByTime)
    val sample = fixture.seed
    val expectedAvg = {
      val data = Seq(sample(2).timestamp - sample(0).timestamp, 0L, sample(5).timestamp - sample(4).timestamp)
      data.sum / data.size
    }
    avg should be(expectedAvg)
  }

  it should "compute unique urls in a session" in {
    val sessionized = App.sessionizeByWindow(sc.parallelize(fixture.seed), App.SESSION_WINDOW)
    val sessionByUrl = App.uniqueUrls(sessionized).collect()
    sessionByUrl should contain theSameElementsAs Seq(
      (("192.168.1.10", 1), 2),
      (("10.17.123.21", 1), 1),
      (("10.17.123.21", 2), 1))
  }

  it should "compute the most engaged visitors" in {
    val sessionized = App.sessionizeByWindow(sc.parallelize(fixture.seed), App.SESSION_WINDOW)
    val sessionByTime = App.sessionByTime(sessionized)
    val engaged = App.mostEngagedVisitors(sessionByTime).collect()
    val sample = fixture.seed
    engaged should contain theSameElementsAs Seq(
      ("192.168.1.10", 1, sample(2).timestamp - sample(0).timestamp),
      ("10.17.123.21", 2, sample(5).timestamp - sample(4).timestamp),
      ("10.17.123.21", 1, 0L))

  }

}
