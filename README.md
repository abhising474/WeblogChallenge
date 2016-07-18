#WebLogChallenge

##Processing & Analytical goals:

- Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window. https://en.wikipedia.org/wiki/Session_(web_analytics)

- Determine the average session time

- Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

- Find the most engaged users, ie the IPs with the longest session times

## Solution
- Using [Apache Spark](http://spark.apache.org/docs/latest/) v1.6.2
- Using Scala v2.10 Sbt v0.13
- Scalatest

###The project can be run in the following two ways :
####Spark Shell
- Download Spark from http://spark.apache.org/downloads.html
- open Spark Shell using ./bin/spark-shell
- copy the code snippet from `src/main/scala/com/weblogchallenge/script.sc`
- update the input file location on line #89
- do not copy lines #9-11 as sc object is already provided by spark shell 
- enter paste mode in spark shell using `:paste`
- hit Ctrl + D after pasting the code
- wait for spark to finish the execution
- script saves the results of the computations as text files in `java.io.tempDir/weblogchallenge` directory, 
  which is also printed when the script is first run
- results from one of executions is included in the [output](output/) directory of the project.

####Spark Cluster Mode
- the project can be build as jar and submitted to spark cluster using ./bin/spark-submit
- [see here for more details](http://spark.apache.org/docs/latest/programming-guide.html#launching-spark-jobs-from-java--scala)
- to build the project jar run `sbt package`

### Running Unit Tests
- run `sbt test`
