import AssemblyKeys._

organization := "me.glorysdj.cabiria"

name := "interactive-spark-sql"

version := "0.1"

scalaVersion := "2.10.4"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Local Maven Repository" at "file:///"+Path.userHome+"/.m2/repository"

//resolvers += "Twitter Maven Repository" at "http://maven.twttr.com"

resolvers += "Scala-tools Maven2 Repository" at "http://scala-tools.org/repo-releases"

resolvers += "Hadoop Releases" at "https://repository.cloudera.com/content/repositories/releases"

resolvers += "Cloudera Repos" at "https://repository.cloudera.com/artifactory/cloudera-repos"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-sql" % "1.0.0-cdh5.1.0" % "provided" withSources()
)

publishTo := Some(Resolver.file("file",  new File(Path.userHome+"/.m2/repository")))

assemblySettings

assemblyOption in assembly ~= { _.copy(includeScala = false) }