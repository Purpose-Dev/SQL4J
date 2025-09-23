lazy val zioVersion = "2.1.21"

name := "sql4j-core"

libraryDependencies ++= Seq(
		"dev.zio" %% "zio" % zioVersion,
		"dev.zio" %% "zio-test" % zioVersion % Test,
		"dev.zio" %% "zio-test-sbt" % zioVersion % Test
)
