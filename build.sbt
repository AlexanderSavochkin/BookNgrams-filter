name := "Google N-Grams filter for ChWiSe.Net"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

libraryDependencies += "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.16"

resolvers += Resolver.sonatypeRepo("public")

resolvers += "Twitter Maven" at "http://maven.twttr.com"