import sbt.project

lazy val dynamoDbTransactions =
  Project(
    id = "dynamodb-transactions",
    base = file(".")
  )
  .settings(
    version := "1.1.2",
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk" % "1.10.51", // 1.9.16
      "junit" % "junit" % "4.11" % Test
    ),
    publishArtifact in (Compile, packageDoc) := false,
    publishArtifact in packageDoc := false,
    sources in (Compile,doc) := Seq.empty
  )
