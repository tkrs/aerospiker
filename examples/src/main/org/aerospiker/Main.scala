package org.aerospiker

import scala.concurrent.duration._

object Main extends App {

  val policy = ClientPolicy()
  val host = Host("127.0.0.1", 3000) :: Nil,
  val client = Client(policy, hosts)

  val key = new Key("test", "teste", "testee")

  { // String
    val bin = new Bin("nickname", new Value("tkrs"))
    val ret = client.put(key, bin).run.runFor(Duration(100, "millis"))
  }

  { // List
    val bin = new Bin("addr", new Value(List("tokyo", "japan")))
    val ret = client.put(key, bin).run.runFor(Duration(1000, "millis"))
  }

  { // Map
    import java.util.ArrayList
    var arr = new ArrayList[String]()
    val n: String = null
    Array("scala", "rust", n, "haskell") foreach (arr.add(_))
    val key = new Key("test", "teste", "testee")
    val bin = new Bin("attribute", new Value(Map("gender" -> "man", "age" -> "30", "lang" -> arr)))
    val ret = client.put(key, bin).run.runFor(Duration(1000, "millis"))
  }

  { // set expiration time
    val ret = client.get(key).run.runFor(Duration(1000, "millis"))
    ret match {
      case -\/(_) => println("xxx")
      case \/-(x) => println(x)
    }
  }

  { // removed record
    val wp = WritePolicy(expiration = 1)
    val ret = client.touch(key)(wp).run.runFor(Duration(1000, "millis"))
    Thread.sleep(2000)
    client.get(key).run.runFor(Duration(1000, "millis")) match {
      case -\/(x) => println(x)
      case \/-(_) => println("x")
    }
  }

  client.close()

}


