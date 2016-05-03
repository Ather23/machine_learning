

name := "spark-scala-trunk-sbt"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion ="1.6.1"

libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.hbase" % "hbase" % "1.2.1",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1"
)

//resolvers += "ClouderaRepo" at "https://repository.cloudera.com/content/repositories/releases"
//
//ivyXML :=
//  <dependencies>
//    <exclude module="thrift"/>
//  </dependencies>