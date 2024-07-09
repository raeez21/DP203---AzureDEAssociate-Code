import scala.util.Random //import the Random module

/*The below code is the defualt code from the sbt skeleton
@main def hello(): Unit =
  println("Hello world!")
  println(msg)

def msg = "I was compiled by Scala 3. :)"
*/
@main def dataApp() =
  //declaring a immutable variable using val 
  val num = 12
  println(num)
  
  //if stmts
  println("\nIf stmts")
  if(num<10)
  {
    println("the number is less than 10")
  }
  else
    println("the number is greater than 10")

  //for construct
  println("\nFor construct")
  var i = 0 //mutable variable
  for(i <- 2 to 10) //from i=2 till i=10
    println("Value of i is: "+ i)

  //while construct
  println("\nwhile construct")
  i = 1
  while(i <= 10){
    println("Value of i is: "+ i)
    i=i+1 //for construct automatically increments value of iterator i, but in while construct we need to increment manually
  }

  //case construct
  println("\nCase construct")
  val x:Int = Random.nextInt(5) //type cast the variable using Int
  x match
    case 1 => println("one")
    case 2 => println("Two")
    case 3 => println("Three")
    case 4 => println("Four")
    case 5 => println("Five")

  //functions
  println("\nSum using function is: "+addNumbers(6,7))
  def addNumbers(x: Int, y: Int): Int = //x,y are arguments and the final one is return type
    return(x+y)


  //List of integers
  val numbers:List[Int] = List(10,20,30,40) 
  println("The head of the list is: "+ numbers.head)
  println("\nPrinting the list of int using for each")
  numbers.foreach{println}

  //List of strings
  val strings:List[String] = List("UserA","UserB","UserC")
  println("\nPrinting the list of strings using for each")
  strings.foreach{println}
  println("User at index 1 is: "+ strings(1))
  //find index value of a particular element
  println("Index value of UserC is: "+ strings.indexOf("UserC"))