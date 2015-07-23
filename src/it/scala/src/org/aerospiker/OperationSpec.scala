package org.aerospiker

import scala.concurrent.duration._
import scala.concurrent.Await

import scalaz._, Scalaz._
import scalaz.concurrent._

import org.scalatest._
import org.scalatest.Assertions._

import Conversions._
import policy._

class OperationSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val hosts = (
      ("AEROSPIKE_SERVER_PORT_3000_TCP_ADDR", "AEROSPIKE_SERVER_PORT_3000_TCP_PORT") ::
      ("AEROSPIKE_SERVER_PORT_3001_TCP_ADDR", "AEROSPIKE_SERVER_PORT_3001_TCP_PORT") ::
      ("AEROSPIKE_SERVER_PORT_3002_TCP_ADDR", "AEROSPIKE_SERVER_PORT_3002_TCP_PORT") ::
      ("AEROSPIKE_SERVER_PORT_3003_TCP_ADDR", "AEROSPIKE_SERVER_PORT_3003_TCP_PORT") ::
      Nil
    ) map {
      case (h, p) => (sys.env.getOrElse(h, ""), sys.env.getOrElse(p, ""))
    } filter {
      case (_, "") => false
      case ("", _) => false
      case _ => true
    } map {
      case (h, p) => new Host(h, p.toInt)
    }

  val key1 = new Key("test", "teste", "testee1")
  val key2 = new Key("test", "teste", "testee2")
  val key3 = new Key("test", "teste", "testee3")
  val key4 = new Key("test", "teste", "testee4")
  val key5 = new Key("test", "teste", "testee5")

  after {
    val keys = key1 :: key2 :: key3 :: key4 :: key5 :: Nil
    val policy = new ClientPolicy()
    val client = Client(policy, hosts)
    keys foreach { client.delete(_).run.start }
    client.close()
  }

  // --------------------------------------------------
  // TEST data
  val nickanme = new Bin("nickname", new Value("tkrs"))

  val attribute = new Bin(
    "attribute", new Value(
      Map("attr" -> List(
        "100-1000",
        "japan",
        "tokyo"))))

  val favorite = new Bin("favorite", new Value(
    List(
      Map("programming" -> List("rust", "scala", "haskell"))
    )
  ))

  val allData = new Bin("data", new Value(
    List("string", true, null, 1e9, (1L << 63) - 1, 0.1234568.toFloat, (1 << 33) - 1, Array(0x02.toByte))
  ))

  it should "acquire written record" in {

    val policy = new ClientPolicy()
    val client = Client(policy, hosts)

    {
      val r1 = client.put(key1, nickanme, attribute).run.runFor(Duration(500, "millis"))
      r1 match {
        case \/-(x) => assert(true)
        case -\/(_) => fail()
      }

      val r2 = client.get(key1).run.runFor(Duration(500, "millis"))
      r2 match {
        case \/-(x) => {
          assert(x.bins.contains("nickname"))
          assert(x.bins.contains("attribute"))
          assert(x.bins.keys.size == 2)
        }
        case -\/(_) => fail()
      }
    }

    {
      val r1 = client.add(key1, favorite, allData).run.runFor(Duration(500, "millis"))
      r1 match {
        case \/-(x) => assert(true)
        case -\/(_) => fail()
      }

      val r2 = client.get(key1).run.runFor(Duration(500, "millis"))
      r2 match {
        case \/-(x) => {
          assert(x.bins.contains("nickname"))
          assert(x.bins.contains("attribute"))
          assert(x.bins.contains("favorite"))
          assert(x.bins.contains("data"))
          assert(x.bins.keys.size == 4)
        }
        case -\/(_) => fail()
      }

      val r3 = client.get(key1, "nickname", "data").run.runFor(Duration(500, "millis"))
      r3 match {
        case \/-(x) => {
          assert(x.bins.contains("nickname"))
          assert(x.bins.contains("data"))
          assert(x.bins.keys.size == 2)
        }
        case -\/(_) => fail()
      }
    }

    {
      val bin = Bin("x", Value("1"))
      val r1 = client.prepend(key1, favorite, bin).run.runFor(Duration(500, "millis"))
      r1 match {
        case \/-(_) => fail()
        case -\/(_) => assert(true)
      }

      val r2 = client.prepend(key1, bin).run.runFor(Duration(500, "millis"))
      r2 match {
        case \/-(x) => assert(true)
        case -\/(_) => fail()
      }

      val r3 = client.get(key1).run.runFor(Duration(500, "millis"))
      r3 match {
        case \/-(x) => {
          assert(x.bins.contains("nickname"))
          assert(x.bins.contains("attribute"))
          assert(x.bins.contains("favorite"))
          assert(x.bins.contains("data"))
          assert(x.bins.contains("x"))
          assert(x.bins.keys.size == 5)
        }
        case -\/(_) => fail()
      }
    }

    {
      val bin = Bin("y", Value("2"))
      val r1 = client.append(key1, favorite, bin).run.runFor(Duration(500, "millis"))
      r1 match {
        case \/-(_) => fail()
        case -\/(_) => assert(true)
      }

      val r2 = client.append(key1, bin).run.runFor(Duration(500, "millis"))
      r2 match {
        case \/-(_) => assert(true)
        case -\/(_) => fail()
      }

      val r3 = client.get(key1).run.runFor(Duration(500, "millis"))
      r3 match {
        case \/-(x) => {
          assert(x.bins.contains("nickname"))
          assert(x.bins.contains("attribute"))
          assert(x.bins.contains("favorite"))
          assert(x.bins.contains("data"))
          assert(x.bins.contains("x"))
          assert(x.bins.contains("y"))
          assert(x.bins.keys.size == 6)
        }
        case -\/(_) => fail()
      }
    }

    {
      val r1 = client.exists(key1).run.runFor(Duration(500, "millis"))
      r1 match {
        case \/-(x) => assert(x)
        case -\/(_) => fail()
      }

      val r2 = client.delete(key1).run.runFor(Duration(500, "millis"))
      r2 match {
        case \/-(x) => assert(x)
        case -\/(_) => fail()
      }
    }

    {
      val r1 = client.exists(key1).run.runFor(Duration(500, "millis"))
      r1 match {
        case \/-(x) => assert(!x)
        case -\/(_) => fail()
      }

      val r2 = client.delete(key1).run.runFor(Duration(500, "millis"))
      r2 match {
        case \/-(x) => assert(!x)
        case -\/(_) => fail()
      }
    }

    client.close()
  }

  it should "remove expired record" in {

    val policy = new ClientPolicy()
    val client = Client(policy, hosts)

    {
      val wp1 = new WritePolicy()
      wp1.expiration = 5
      client.put(key1, nickanme, attribute)(wp1).run.runFor(Duration(500, "millis"))
      client.put(key2, favorite, allData).run.runFor(Duration(500, "millis"))

      client.exists(key1).run.runFor(Duration(500, "millis")) match {
        case \/-(x) => assert(x)
        case -\/(_) => fail()
      }
      client.exists(key2).run.runFor(Duration(500, "millis")) match {
        case \/-(x) => assert(x)
        case -\/(_) => fail()
      }

      val wp2 = new WritePolicy()
      wp2.expiration = 2
      val r1 = client.touch(key2)(wp2).run.runFor(Duration(500, "millis"))
      r1 match {
        case \/-(x) => assert(true)
        case -\/(_) => fail()
      }

      Thread.sleep(3000)

      client.exists(key1).run.runFor(Duration(500, "millis")) match {
        case \/-(x) => assert(x)
        case -\/(_) => fail()
      }
      client.exists(key2).run.runFor(Duration(500, "millis")) match {
        case \/-(x) => assert(!x)
        case -\/(_) => fail()
      }

      Thread.sleep(3000)

      client.exists(key1).run.runFor(Duration(500, "millis")) match {
        case \/-(x) => assert(!x)
        case -\/(_) => fail()
      }
      client.exists(key2).run.runFor(Duration(500, "millis")) match {
        case \/-(x) => assert(!x)
        case -\/(_) => fail()
      }

    }

    client.close()
  }

  it should "present all bins that the 'put' to the async" in {

    val policy = new ClientPolicy()
    val client = Client(policy, hosts)

    {
      client.put(key3, nickanme).run.runAsync { a =>
        println("nickname")
      }
      client.put(key3, attribute).run.runAsync { a =>
        println("attribute")
      }
      client.put(key3, favorite).run.runAsync { a =>
        println("favorite")
      }
      client.put(key3, allData).run.runAsync { a =>
        println("allData")
      }

      Thread.sleep(200)

      val r1 = client.get(key3).run.runFor(Duration(500, "millis"))

      r1 match {
        case \/-(x) => {
          assert(x.bins.contains("nickname"))
          assert(x.bins.contains("attribute"))
          assert(x.bins.contains("favorite"))
          assert(x.bins.contains("data"))
          assert(x.bins.keys.size == 4)
        }
        case -\/(_) => fail()
      }

    }
  }

  it should "called to registred UDF" in {
    val policy = new ClientPolicy()
    val client = Client(policy, hosts)

    val both = client.register("lua/test.lua", "test.lua").run.runFor(Duration(500, "millis"))
    both match {
        case \/-(task) => task.waitTillComplete()
        case -\/(_) => fail()
    }

    val r1 = client.execute[String](key3, "test", "hello_world").run.runFor(Duration(500, "millis"))
    r1 match {
      case \/-(msg) => assert(msg == "Hello World!!")
      case -\/(_) => fail()
    }

    val r2 = client.removeUdf("test.lua").run.runFor(Duration(500, "millis"))
    r2 match {
      case \/-(_) => assert(true)
      case -\/(_) => fail()
    }

    Thread.sleep(1000)

    val r3 = client.execute[String](key3, "test", "hello_world").run.runFor(Duration(500, "millis"))
    println(r3)
    r3 match {
      case \/-(_) => fail()
      case -\/(_) => assert(true)
    }

  }

  it should "responded errror if key or namespace or set is unregistered" in {

    val policy = new ClientPolicy()
    val client = Client(policy, hosts)

    {
      val key = new Key("test", "teste", "not")
      val result = client.get(key).run.runFor(Duration(500, "millis"))

        result match {
          case \/-(_) => fail()
          case -\/(_) => assert(true)
        }
    }

    {
      val key = new Key("test", "not", "not")
      val result = client.get(key).run.runFor(Duration(500, "millis"))

      result match {
        case \/-(_) => fail()
        case -\/(_) => assert(true)
      }
    }

    {
      val key = new Key("not", "not", "not")
      val result = client.get(key).run.runFor(Duration(500, "millis"))

      result match {
        case \/-(_) => fail()
        case -\/(_) => assert(true)
      }
    }

    client.close()

  }

  it should "throw java.net.ConnectException if specify a incorrect host" in {
    try {
      val errHosts = new Host("127.0.0.1", 9090) :: Nil
      val policy = new ClientPolicy()
      val client = Client(policy, errHosts)
      fail("Unexpected connection")
    } catch {
      case _ => assert(true)
    }
  }

}
