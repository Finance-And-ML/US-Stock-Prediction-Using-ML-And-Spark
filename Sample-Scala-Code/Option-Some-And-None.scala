// Use Option when things can be null
//    Option object is either a instance of Some or None.

// define the name, type as a variable placeholder
var random_str = None:Option[String]
random_str = Some("This is a random string")
println(random_str)
println(random_str.getOrElse("Nothing")) // print: This is a random String
random_str = None
println(random_str.getOrElse("Nothing")) // print: Nothing

// Define the function that returns an Option[Int]
def toInt(in: String): Option[Int] = {
    try {
        Some(Integer.parseInt(in.trim))
    } catch {
        case e: NumberFormatException => None
    }
}

random_str = Some("123")
random_str.map(toInt).foreach(println)

// Use case 1
val int_str = "123"
toInt(int_str) match {
  case Some(i) => println(i)
  case None => println("Not a Int")
}

// Use case 2
val bag = List("1", "2", "foo", "3", "bar")
val sum = bag.flatMap(toInt).sum
