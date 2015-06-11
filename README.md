# aerospiker

[![Build Status](https://travis-ci.org/tkrs/aerospiker.svg?branch=master)](https://travis-ci.org/tkrs/aerospiker)

This is a Aerospike client implementation for scala.

It is just a wrapper to [aerospike-java-client](https://github.com/aerospike/aerospike-client-java)

## Example

```scala
package org.aerospiker

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {

  val settings = Settings(
    host = "127.0.0.1:3000",
    user = "",
    pwd = "",
    maxRetries = 3)

  val client = Client(settings)
  val key = new Key("test", "teste", "testee")

  { // String
    val bin = new Bin("nickname", new Value("tkrs"))
    val f = client.put(key, bin).run
    f onSuccess {
      case msg => println("string put done")
    }
    Await.result(f, Duration(100, "millis"))
  }

  { // List
    val bin = new Bin("addr", new Value(List("tokyo", "japan")))
    val f = client.put(key, bin).run
    f onSuccess {
      case msg => println("list put done")
    }
    Await.result(f, Duration(1000, "millis"))
  }

  { // Map
    import java.util.ArrayList
    var arr = new ArrayList[String]()
    val n: String = null
    Array("scala", "rust", n, "haskell") foreach (arr.add(_))
    val key = new Key("test", "teste", "testee")
    val bin = new Bin("attribute", new Value(Map("gender" -> "man", "age" -> "30", "lang" -> arr)))
    val f = client.put(key, bin).run
    f onSuccess {
      case msg => println("map put done")
    }
    Await.result(f, Duration(1000, "millis"))
  }

  {
    val f = client.get(key).run
    f onSuccess {
      case msg => println(msg)
    }
    Await.result(f, Duration(1000, "millis"))
  }
  client.close()

}
```

## Support

### Operation

* get

* put

### DataType

* Integer

* String

* Map (nested)

* List (nested) 

### TODO

* More support operation

* Test
			
* Document

* Benchmark

* Erasure to warning
