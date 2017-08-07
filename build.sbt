name := "bdml"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0",
  "org.apache.spark" % "spark-hive_2.11" % "2.0.0" % "provided"
)

libraryDependencies ++= Seq(
  "org.scalanlp" % "breeze_2.11" % "0.12",
  "org.scalanlp" % "breeze-natives_2.11" % "0.12"
)

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.0.0"

// libraryDependencies += "com.crealytics" % "spark-excel_2.11" % "0.8.2"

libraryDependencies += "org.apache.poi" % "poi" % "3.14"

// https://mvnrepository.com/artifact/org.vegas-viz/vegas_2.11
libraryDependencies += "org.vegas-viz" % "vegas_2.11" % "0.3.9"
libraryDependencies += "org.scalafx" % "scalafx_2.11" % "8.0.102-R11"
