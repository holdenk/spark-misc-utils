val sparkVersion = settingKey[String]("Spark version")
val sparkUtilsVersion = settingKey[String]("Spark Utils version")

lazy val core = (project in file("."))
  .settings(
    name := "spark-misc-utils",
    commonSettings,
    coreSources,
    publishSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"        % sparkVersion.value,
      "org.apache.spark" %% "spark-streaming"   % sparkVersion.value,
      "org.apache.spark" %% "spark-sql"         % sparkVersion.value,
      "org.apache.spark" %% "spark-hive"        % sparkVersion.value,
      "org.apache.spark" %% "spark-catalyst"    % sparkVersion.value,
      "org.apache.spark" %% "spark-yarn"        % sparkVersion.value,
      "org.apache.spark" %% "spark-mllib"       % sparkVersion.value,
      "org.apache.iceberg" % "iceberg-api" % "0.12.0",
      "org.apache.iceberg" % "iceberg-hive-metastore" % "0.12.0"
    ),
    libraryDependencies ++= {
      if (scalaVersion.value > "2.12.0") {
        Seq("com.holdenkarau" %% "spark-testing-base" %  s"${sparkVersion.value}_1.1.0" % "test")
      } else {
        Seq()
      }
    }
  )

val coreSources = unmanagedSourceDirectories in Compile  := {
  if (sparkVersion.value < "3.0.0") Seq(
    (sourceDirectory in Compile)(_ / "/scala"),
    (sourceDirectory in Compile)(_ / "2.4/scala"),
  ).join.value
  else
    Seq(
      (sourceDirectory in Compile)(_ / "/scala"),
      (sourceDirectory in Compile)(_ / "3.0/scala")
    ).join.value
}


val commonSettings = Seq(
  organization := "com.holdenkarau",
  publishMavenStyle := true,
  sparkUtilsVersion := "0.0.1",
  sparkVersion := System.getProperty("sparkVersion", "2.4.0"),
  version := sparkVersion.value + "_" + sparkUtilsVersion.value,
  scalaVersion := {
    "2.12.12"
  },
  crossScalaVersions := {
    if (sparkVersion.value >= "3.0.0") {
      Seq("2.12.12")
    } else {
      Seq("2.12.12", "2.11.11")
    }
  },
  skip in test := {
    scalaVersion.value < "2.12.0"
  },
  scalacOptions ++= Seq("-deprecation", "-unchecked", "-Yrangepos", "-Ywarn-unused-import"),
  javacOptions ++= {
    Seq("-source", "1.8", "-target", "1.8")
  },
  javaOptions ++= Seq("-Xms6G", "-Xmx6G", "-XX:MaxPermSize=4048M", "-XX:+CMSClassUnloadingEnabled"),

  parallelExecution in Test := false,
  fork := true,

  scalastyleSources in Compile ++= {unmanagedSourceDirectories in Compile}.value,
  scalastyleSources in Test ++= {unmanagedSourceDirectories in Test}.value,

  resolvers ++= Seq(
    "JBoss Repository" at "https://repository.jboss.org/nexus/content/repositories/releases/",
    "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
    "Twitter Maven Repo" at "https://maven.twttr.com/",
    "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
    "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
    "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
    "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
    "Mesosphere Public Repository" at "https://downloads.mesosphere.io/maven",
    Resolver.sonatypeRepo("public")
  )
)

// publish settings
lazy val publishSettings = Seq(
  pomIncludeRepository := { _ => false },
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },

  licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),

  homepage := Some(url("https://github.com/holdenk/spark-misc-utils")),

  scmInfo := Some(ScmInfo(
    url("https://github.com/holdenk/spark-misc-utils.git"),
    "scm:git@github.com:holdenk/spark-misc-utils.git"
  )),

  developers := List(
    Developer("holdenk", "Holden Karau", "holden@pigscanfly.ca", url("http://www.holdenkarau.com"))
  ),

  //credentials += Credentials(Path.userHome / ".ivy2" / ".spcredentials")
  credentials ++= Seq(Credentials(Path.userHome / ".ivy2" / ".sbtcredentials"), Credentials(Path.userHome / ".ivy2" / ".sparkcredentials")),
  useGpg := true,
  artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
    artifact.name + "-" + sparkVersion.value +  module.revision + "." + artifact.extension
  }
)


lazy val noPublishSettings =
  skip in publish := true
