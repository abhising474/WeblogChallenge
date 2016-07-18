name := "WeblogChallenge"

version := "1.0"

scalaVersion := "2.10.6"

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.2",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)




    