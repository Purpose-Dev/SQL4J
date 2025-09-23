ThisBuild / scalaVersion := "3.3.1"

lazy val core = (project in file("."))
	.settings(
			name := "sql4j-core",
			libraryDependencies ++= Seq(
					"dev.zio" %% "zio" % "2.1.21",
					"dev.zio" %% "zio-test" % "2.1.21" % Test,
					"dev.zio" %% "zio-test-sbt" % "2.1.21" % Test
			)
	)

