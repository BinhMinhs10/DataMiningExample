object DataStructure extends App{
  val ages = Seq(42, 75, 29, 64)
  println(s"The oldest person is ${ages.max}")

  println({
    val x = 1 + 1
    x + 1
  })

  val getTheAnswer = (x: Int, y: Int) => x + y
  println(getTheAnswer(1,2))

  /* Array in scala*/
  var name = Array("Faizan", "Swati", "Kavya", "Deepak")
  /* mkString will convert collection to string representations*/
  println(name.mkString(" "))
  println(name(2).mkString(""))

  var nameNull = new Array[String](4)
  println(nameNull.mkString(" "))

  /* List */
  val numbers = List(1,6,4)
  println("List is: "+ numbers)
  for(element <- numbers){
    println("index have value: "+element)
  }




}
