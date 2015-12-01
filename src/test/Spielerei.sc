val l = List[String]("a", "b", null, "c")

l.flatMap(s => Option(s))

Some("test").toTraversable.flatMap(_.toCharArray)