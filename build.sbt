import sbt.Keys.testFrameworks

lazy val Benchmark = config("bench") extend Test

name := "clickhouse-scala-core"

version := "0.0.2"

scalaVersion := "2.12.4"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding",
  "utf-8",
  "-explaintypes",
  "-feature",
  "-language:existentials",
  "-unchecked",
  "-Xcheckinit",
  "-Xfuture",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-extra-implicit",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-Ywarn-nullary-override",
  "-Ywarn-nullary-unit",
  "-Ywarn-unused:imports",
  "-Ywarn-value-discard"
)

val scalaMeterFramework = new TestFramework("org.scalameter.ScalaMeterFramework")

lazy val core = project
  .in(file("."))
  .settings(
    libraryDependencies ++= Seq(
      "org.reactivestreams" % "reactive-streams" % "1.0.1",
      "org.testcontainers" % "testcontainers" % "1.4.2" % "test",
      "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.1.34" % "test",
      "org.scalatest" %% "scalatest" % "3.0.4" % "test",
      "com.storm-enroute" %% "scalameter" % "0.8.2" % "bench",
      "ch.qos.logback" % "logback-classic" % "1.1.3" % "test"
    )
  )
  .configs(Benchmark)
  .settings(
    inConfig(Benchmark)(
      Defaults.testSettings ++ Seq(
        testFrameworks := Seq(scalaMeterFramework),
        testOptions in Benchmark += Tests.Argument("-silent"),
        parallelExecution in Benchmark := false,
        logBuffered := false
      )
    ): _*
  )
