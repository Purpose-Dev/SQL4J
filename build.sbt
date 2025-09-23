lazy val root = (project in file("."))
	.aggregate(core)
	.settings(
			name := "sql4j",
			scalaVersion := "3.3.1"
	)

lazy val core = project.in(file("sql4j-core"))
// lazy val memory = project.in(file("sql4j-memory")).dependsOn(core)
