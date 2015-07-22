package org.aerospiker

import scala.concurrent.duration._
import scala.concurrent.Await

import scalaz._, Scalaz._
import scalaz.concurrent._

import org.scalatest._
import org.scalatest.Assertions._

import Conversions._
import policy._

class OperationSpec extends FlatSpec with Matchers {

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
      case (h, p) => Host(h, p.toInt)
    }

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

  it should "put, get, delete, append values" in {

    val key = new Key("test", "teste", "testee")
    val policy = ClientPolicy()

    val client = Client(policy, hosts)

    {
      // put with expiration -> get -> get(notfound)

      import policy._
      val wp = WritePolicy(expiration = 2)
      val result = client.put(key, nickanme, attribute)(wp)
        .run
        .runFor(Duration(500, "millis"))

      result match {
        case \/-(_) => assert(true)
        case -\/(_) => fail()
      }

      val r2 = client.get(key)
        .run
        .runFor(Duration(500, "millis"))

      r2 match {
        case \/-(_) => assert(true)
        case -\/(_) => fail()
      }

      Thread.sleep(3000)

      val r3 = client.get(key)
        .run
        .runFor(Duration(500, "millis"))

      r3 match {
        case \/-(_) => fail()
        case -\/(_) => assert(true)
      }
    }

    {
      // put -> touch with expiration -> get -> get(notfound)

      import policy._
      val _ = client.put(key, nickanme)
        .run
        .runFor(Duration(500, "millis"))

      val wp = WritePolicy(expiration = 2)
      val r1 = client.touch(key)(wp)
        .run
        .runFor(Duration(500, "millis"))

      r1 match {
        case \/-(_) => assert(true)
        case -\/(_) => fail()
      }

      val r2 = client.get(key)
        .run
        .runFor(Duration(500, "millis"))

      r2 match {
        case \/-(_) => assert(true)
        case -\/(_) => fail()
      }

      Thread.sleep(3000)

      val r3 = client.get(key)
        .run
        .runFor(Duration(500, "millis"))

      r3 match {
        case \/-(_) => fail()
        case -\/(_) => assert(true)
      }
    }

    {
      // put -> append -> get -> delete

      val _ = client.put(key, nickanme, attribute)
        .run
        .runFor(Duration(500, "millis"))

      val r1 = client.append(key, favorite, allData)
        .run
        .runFor(Duration(500, "millis"))

      r1 match {
        case \/-(_) => assert(true)
        case -\/(_) => fail()
      }

      val r2 = client.get(key)
        .run
        .runFor(Duration(500, "millis"))

      r2 match {
        case \/-(x) => assert(x.bins.contains("nickname") &&x.bins.contains("attribute") &&  x.bins.contains("favorite") && x.bins.contains("data"))
        case -\/(_) => fail()
      }

      val r3 = client.delete(key)
        .run
        .runFor(Duration(500, "millis"))
        r3 match {
          case \/-(r) => assert(r)
          case -\/(_) => fail()
        }

      val r4 = client.get(key)
        .run
        .runFor(Duration(500, "millis"))

      r4 match {
        case \/-(_) => fail()
        case -\/(_) => assert(true)
      }
    }

    client.close()

  }

  it should "get with async" in {

    val key = new Key("test", "teste", "testee")
    val policy = ClientPolicy()

    val client = Client(policy, hosts)

    {
      client.put(key, nickanme).run.runAsync { a =>
        println("nickname")
      }
      client.put(key, attribute).run.runAsync { a =>
        println("attribute")
      }
      client.put(key, favorite).run.runAsync { a =>
        println("favorite")
      }
      client.put(key, allData).run.runAsync { a =>
        println("allData")
      }

      Thread.sleep(200)

      val r2 = client.get(key)
        .run
        .runFor(Duration(500, "millis"))

      r2 match {
        case \/-(x) => assert(x.bins.contains("nickname") &&x.bins.contains("attribute") &&  x.bins.contains("favorite") && x.bins.contains("data"))
        case -\/(_) => fail()
      }

    }
  }

  it should "Error result if unregister key & namespace & set" in {

    val policy = ClientPolicy()
    val client = Client(policy, hosts)

    {
      val key = new Key("test", "teste", "not")
      val result = client.get(key)
        .run
        .runFor(Duration(500, "millis"))

        result match {
          case \/-(_) => assert(false)
          case -\/(_) => assert(true)
        }
    }

    {
      val key = new Key("test", "not", "not")
      val result = client.get(key)
        .run
        .runFor(Duration(500, "millis"))

      result match {
        case \/-(_) => assert(false)
        case -\/(_) => assert(true)
      }
    }

    {
      val key = new Key("not", "not", "not")
      val result = client.get(key)
        .run
        .runFor(Duration(500, "millis"))

      result match {
        case \/-(_) => assert(false)
        case -\/(_) => assert(true)
      }
    }

    client.close()

  }

  it should "throw java.net.ConnectException if specify a incorrect host" in {
    try {
      val errHosts = Host("127.0.0.1", 9090) :: Nil
      val policy = ClientPolicy()
      val client = Client(policy, errHosts)
      fail("Unexpected connection")
    } catch {
      case _ => assert(true)
    }
  }

}
