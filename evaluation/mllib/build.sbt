name := "MLLibAlgs"

version := "0.1"

scalaVersion := "2.12.16"

// resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq (
    "org.apache.spark" %% "spark-core" % "3.3.2" % "provided",
    "org.apache.spark" %% "spark-streaming" % "3.3.2" % "provided",
    "org.apache.spark" %% "spark-mllib" % "3.3.2" % "provided",
    "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided",
//    "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
//    "com.github.fommil.netlib" % "netlib-native_system-linux-x86_64" % "1.1" pomOnly(),
//    "com.github.fommil.netlib" % "netlib-native_ref-linux-x86_64" % "1.1" pomOnly()
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
