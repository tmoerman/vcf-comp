val l = List[String]("a", "b", null, "c")

l.flatMap(s => Option(s))

