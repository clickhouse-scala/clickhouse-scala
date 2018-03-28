## What is this? 
Alternative driver for [ClickHouse](https://github.com/yandex/ClickHouse).

## What is project status?
Temprorarily freezed. This version worked fine for custom loads, but there were quite specific requirements. Pull/feature requests and any other questions are much appreciated. Authors contunie working on it, but in relatively slow pace.

## Why? It already has official Java jdbc (via http) driver!
* This one communicates with CH via native tcp protocol.
  That offers more features (e.g. query progress) and possibilities for optimizations.
* Sometime you don't need a jdbc.
* Core module has zero dependencies.

## Is this Scala-specific implementation? Can I call it from Java?
Project is divided in several modules. This one, 'core', contains only basic protocol implementation and has seamless compatibility with java (and no external dependencies).
Other modules (integration with reactive-streams, jdbc layer, automatic derivation of decoders) will reside in neighboring repositories.

## What features do you have in mind?
* [x] support all CH datatypes (including multidimensional arrays), selects / inserts / ddl
* [x] use 'reactive streams api'
* [ ] nio (netty?)
* [ ] pool of connections (load balancing)
* [ ] ssl / tls
* [ ] streaming selects / inserts
* [ ] jdbc layer
* [ ] automatic derivation of decoders (for Scala)
* [ ] compression (in CH terms)

## You said it has better performance, show me tests and numbers!
[Integration tests](src/test) and [load tests](src/bench).
You can also run them with `sbt test` or `sbt bench:test` (bunch of testcontainers-based tests).

## Can I use it now?
At the moment, version is **0.0.2**, and it's not ready (generally speaking) for production. However, you can play around with it, send feature / pull requests, use it for inspiration.
