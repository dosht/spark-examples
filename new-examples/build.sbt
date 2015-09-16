name := "Study Examples"

scalaVersion in ThisBuild := "2.11.6"

sbtVersion := "0.14.0"

shellPrompt in ThisBuild := { p =>
  val currentProjectId = Project.extract(p).currentProject.id
  s"[${scala.Console.CYAN}$currentProjectId${scala.Console.RESET}] $$ "
}

val sparkVersion = "1.5.0"

libraryDependencies in ThisBuild ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion)

lazy val advancedSpark = project in file("advanced-spark")

