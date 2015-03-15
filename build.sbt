name := "ngramsfilter"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test"

libraryDependencies ++= Seq(
    "com.github.scopt" %% "scopt" % "3.3.0",
    "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.16",
    ("org.apache.spark" %% "spark-core" % "1.2.0").
    exclude("org.eclipse.jetty.orbit", "javax.servlet").
    exclude("org.eclipse.jetty.orbit", "javax.transaction").
    exclude("org.eclipse.jetty.orbit", "javax.mail").
    exclude("org.eclipse.jetty.orbit", "javax.activation").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-collections", "commons-collections").
    exclude("com.esotericsoftware.minlog", "minlog")
)

resolvers += Resolver.sonatypeRepo("public")

resolvers += "Twitter Maven" at "http://maven.twttr.com"

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")     => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties"                             => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

