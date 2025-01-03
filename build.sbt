
val toolkitV = "0.1.28"
val toolkit = "org.typelevel" %% "toolkit" % toolkitV
val toolkitTest = "org.typelevel" %% "toolkit-test" % toolkitV

ThisBuild / scalaVersion := "2.13.15"
libraryDependencies += toolkit
libraryDependencies += (toolkitTest % Test)
