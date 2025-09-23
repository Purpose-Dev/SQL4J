lazy val zioVersion = "2.1.21"

name := "sql4j-memory"

libraryDependencies ++= Seq(
		"dev.zio" %% "zio" % zioVersion,
		"dev.zio" %% "zio-streams" % zioVersion,
		"dev.zio" %% "zio-test" % zioVersion % Test,
		"dev.zio" %% "zio-test-sbt" % zioVersion % Test
)
