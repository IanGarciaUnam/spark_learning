
object types extends App{
    
    def printIt(a:Any): 
        Unit = println(a)
    
    val list: List[Any]=List(
        "a string",
        732,
        'c',
        '\'',
        true,
        () => "anonymous function"

)
    list.foreach(element => printIt(element))

}
