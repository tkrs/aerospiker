# aerospiker

[![Stories in Ready](https://badge.waffle.io/tkrs/aerospiker.svg?label=ready&title=Ready)](http://waffle.io/tkrs/aerospiker)
[![codecov.io](http://codecov.io/github/tkrs/aerospiker/coverage.svg?branch=master)](http://codecov.io/github/tkrs/aerospiker?branch=master)

[![wercker status](https://app.wercker.com/status/07c0ec3bd555c18ff328f9f976f3725e/m "wercker status")](https://app.wercker.com/project/bykey/07c0ec3bd555c18ff328f9f976f3725e)

This is a Aerospike client implementation for scala.

It is just a wrapper to [aerospike-java-client](https://github.com/aerospike/aerospike-client-java)

## Test setting

### Requirement

- boot2docker

- docker
  
### Ready

```bash
docker run -tid --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike/aerospike-server # Only first time
# next
docker run ${container id} # docker ps
```

### Run

```bash
sbt clean it:test
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
