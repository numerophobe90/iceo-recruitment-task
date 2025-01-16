ThisBuild / organization := "com.example"
ThisBuild / scalaVersion := "2.13.15"

lazy val root = (project in file(".")).settings(
  name := "recruitment-task",
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect"      % "3.5.3",
    "co.fs2"        %% "fs2-core"         % "3.9.4",
    "org.tpolecat"  %% "skunk-core"       % "0.6.3",
    "com.beachape"  %% "enumeratum"       % "1.7.3",
    "org.flywaydb"   % "flyway-core"      % "9.21.1",
    "org.postgresql" % "postgresql"       % "42.6.0",
    "org.typelevel" %% "log4cats-slf4j"   % "2.6.0",
    "org.typelevel" %% "log4cats-testing" % "2.6.0" % Test,
    "org.typelevel" %% "log4cats-noop"    % "2.6.0" % Test,
    "ch.qos.logback" % "logback-classic"  % "1.4.14",
    compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % Test
  )
)
