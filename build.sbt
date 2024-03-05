val sparkVersion = "3.5.0"

inThisBuild(Seq(
  organization := "ch.epfl",
  scalaVersion := "3.3.1",
  version := "0.1.1",
  scalacOptions ++= Seq("-deprecation", "-feature")
))

lazy val root = project.in(file("."))
  .settings(
    name := "Project_1_solution",
    initialize := {
      val _ = initialize.value
      val required = "11"
      val current = sys.props("java.specification.version")
      assert(current == required, s"Unsupported JDK. Using $current but requires $required")
    },
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13),
      ("org.apache.spark" %% "spark-mllib" % sparkVersion).cross(CrossVersion.for3Use2_13),

      "org.slf4j" % "slf4j-api" % "1.7.13",
      "org.slf4j" % "slf4j-log4j12" % "1.7.13",

      "org.scalactic" %% "scalactic" % "3.2.17",

      "org.junit.jupiter" % "junit-jupiter-api" % "5.3.1" % Test,
      "org.junit.jupiter" % "junit-jupiter-params" % "5.3.1" % Test,

      // junit tests (invoke with `sbt test`)
      "com.novocode" % "junit-interface" % "0.11" % "test"
    )
  )