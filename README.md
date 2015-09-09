# aerospiker

[![Stories in Ready](https://badge.waffle.io/tkrs/aerospiker.svg?label=ready&title=Ready)](http://waffle.io/tkrs/aerospiker)
[![codecov.io](http://codecov.io/github/tkrs/aerospiker/coverage.svg?branch=master)](http://codecov.io/github/tkrs/aerospiker?branch=master)

[![wercker status](https://app.wercker.com/status/07c0ec3bd555c18ff328f9f976f3725e/m "wercker status")](https://app.wercker.com/project/bykey/07c0ec3bd555c18ff328f9f976f3725e)

This is a Aerospike client implementation for scala.

It is just a wrapper to [aerospike-java-client](https://github.com/aerospike/aerospike-client-java)

current supported 3.1.4

## Getting started

```scala
libraryDependencies ++= Seq(
  "com.github.tkrs" %% "aerospiker-core" % "0.3.0",
  "com.github.tkrs" %% "aerospiker-service" % "0.1.0-SNAPSHOT"
)
```

## Support

### Operation

- put

- append

- prepend

- add

- delete

- touch

- get

- register

- removeUdf

- execute

- getHeader

- scanAll

### DataType

- Basic types

  - Integer

  - String

- complex types

  - Map (nested)

  - List (nested)

### TODO

- More support operation (create, update, replace, query)

- More support data types (Large data types)

- Unit Test

- Document

- Benchmark

- Erasure to warning

## COPYRIGHT

Copyright (c) 2015 Takeru Sato

## LICENSE

MIT
