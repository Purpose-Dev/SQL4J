ThisBuild / scalaVersion := "3.7.3"
ThisBuild / version := "0.0.1-SNAPSHOT"
ThisBuild / organization := "dev.sql4j"

lazy val zioJsonVersion = "0.7.44"

ThisBuild / scalacOptions ++= Seq(
		"-deprecation",
		"-explain-types",
		"-feature",
		"-unchecked",
		"-Xfatal-warnings"
)

lazy val core = project.in(file("modules/sql4j-core"))
lazy val memory = project.in(file("modules/sql4j-memory")).dependsOn(core)

lazy val root = (project in file("."))
	.aggregate(core, memory)
	.settings(
			name := "sql4j",
			publish := {},
			publishLocal := {}
	)
