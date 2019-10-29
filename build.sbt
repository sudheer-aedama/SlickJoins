name := "hello-slick"

scalaVersion := "2.11.11"

mainClass in Compile := Some("HelloSlick")

libraryDependencies ++= List(
  "com.typesafe.slick" %% "slick" % "3.3.2",
  "org.slf4j" % "slf4j-nop" % "1.7.10",
  "com.h2database" % "h2" % "1.4.200",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

fork in run := true
