# aerospiker

[![Stories in Ready](https://badge.waffle.io/tkrs/aerospiker.svg?label=ready&title=Ready)](http://waffle.io/tkrs/aerospiker)
[![codecov.io](http://codecov.io/github/tkrs/aerospiker/coverage.svg?branch=master)](http://codecov.io/github/tkrs/aerospiker?branch=master)

[![wercker status](https://app.wercker.com/status/07c0ec3bd555c18ff328f9f976f3725e/m "wercker status")](https://app.wercker.com/project/bykey/07c0ec3bd555c18ff328f9f976f3725e)

This is a Aerospike client for scala.

It is just a wrapper to [aerospike-java-client](https://github.com/aerospike/aerospike-client-java)

## Getting started

```scala
libraryDependencies ++= Seq(
  "com.github.tkrs" %% "aerospiker-core" % "0.4.0",
  "com.github.tkrs" %% "aerospiker-msgpack" % "0.4.0",
  "com.github.tkrs" %% "aerospiker-task" % "0.4.0"
)
```

## For example

### Map, List

```scala
import aerospiker._
import aerospiker.policy.{ ClientPolicy, WritePolicy }
import aerospiker.task.{ Aerospike, Settings }
import com.typesafe.scalalogging.LazyLogging
import io.circe.generic.auto._

object Standard extends App with LazyLogging {

  case class User(name: String, age: Int, now: Long, bbb: Seq[Double], option: Option[String])
  val u1 = User("bbb", 31, System.currentTimeMillis(), Seq(1.2, 3.4), None)
  val u2 = User("aaa", 24, System.currentTimeMillis(), Seq(1.2, 3.4), Some("OK"))
  val u3 = User("ccc", 3, System.currentTimeMillis(), Seq(1.2, 3.4), Some("OK"))

  val writePolicy = WritePolicy(sendKey = true, timeout = 3000, maxRetries = 5)
  val clientPolicy = ClientPolicy(writePolicyDefault = writePolicy)

  val client = AerospikeClient(clientPolicy, Host("192.168.99.100", 3000))

  val settings = Settings("test", "account", "user")

  import Aerospike._

  val action = for {
    _ <- put(settings, u1)
    get <- get[User](settings)
    del <- delete(settings)
    _ <- puts(settings, Map(u1.name -> u1, u2.name -> u2, u3.name -> u3))
    all <- all[User](settings)
    act <- deletes(settings, Seq(u1.name, u2.name, u3.name))
  } yield {
    println(s"get :: $get")
    println(s"all :: $all")
    act
  }

  println("start")
  println(action.run(client).attemptRun)

  client.close()
}
```

### LargeMap

```scala
import aerospiker._
import aerospiker.policy.{ ClientPolicy, WritePolicy }
import aerospiker.task.{ AerospikeLargeMap, Settings }
import com.typesafe.scalalogging.LazyLogging
import io.circe.generic.auto._

object LargeMap extends App with LazyLogging {

  case class User(name: String, age: Int, now: Long, bbb: Seq[Double])
  type Users = Map[String, User]
  val u1 = User("tkrs", 31, System.currentTimeMillis(), Seq(1.2, 3.4))
  val u2 = User("boo", 5, System.currentTimeMillis(), Seq(1.2, 3.4))
  val u3 = User("xyz", 99, System.currentTimeMillis(), Seq(1.00000002, 3.4))

  val writePolicy = WritePolicy(sendKey = true, timeout = 3000, maxRetries = 5)
  val clientPolicy = ClientPolicy(writePolicyDefault = writePolicy)
  val client = AerospikeClient(clientPolicy, Host("192.168.99.100", 3000))

  val settings = Settings("test", "user", "u1", "bin")

  import AerospikeLargeMap._
  val action = for {
    _ <- put(settings, "u1", u1)
    get <- get[User](settings, "u1")
    del <- delete(settings, "u1")
    _ <- puts(settings, Map(u1.name -> u1, u2.name -> u2, u3.name -> u3))
    all <- all[Users](settings)
    act <- deleteBin(settings)
  } yield {
    println(s"get :: $get")
    println(s"all :: $all")
    act
  }

  println("start")
  println(action.run(client).attemptRun)

  client.close()
}
```

## LICENSE

MIT
