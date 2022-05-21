# SimpleKV

A simple distributed key-value storage system based on bitcask from scratch.

Have minimum dependencies, nearly built from nothing.

This project is a experiment on kafka's distributed system and bitcask storage model. **Do not use this in production!**

## How to run this project

- To run this project, you should have **Apache Zookeeper** running on your OS first.
- Let's run a standalone server as test!  Go to `/conf/client/cli.yaml` and `/conf/standalone/server-standalone.yaml`, and check whether your zookeeper port is correctly configured.

  ```yaml
  zk:
    servers:
      - 127.0.0.1:[YourPortHere]
  ```

- Then go into the root folder of this project, try to boot up a **standalone** server.

  ```bash
  go mod tidy
  make standalone
  ```

- Boot up a CLI client to start operating.
  ```bash
  make cli
  ```

- Try some of the commands:
  ```bash
  127.0.0.1:2181> set hello 1
  OK
  127.0.0.1:2181> get hello
  1
  127.0.0.1:2181> expire hello 200
  OK
  127.0.0.1:2181> ttl hello
  198
  127.0.0.1:2181> del hello
  OK
  127.0.0.1:2181> get hello
  record not found
  ```

## Roadmap

Here are some basic requirements:

- [x] LRU Cache.
- An index system based on either hashmap / skiplist.
  - [x] hashmap
- Support string data structure. Implement GET/SET/EXPIRE/DEL method.
  - [x] Get
  - [x] Set
  - [x] Expire
  - [x] Del
- [x] Appendonly log as disk storage with Bitcask theory.
- [x] Master-slave replication.
  - [x] Controller election.
  - [x] Leader election by comparing offsets.
  - [x] Log replication.
  - [x] ISR management.
- [x] Using zookeeper/etcd as service registration, leader election, etc.
- [x] Using GRPC to communicate between client-server and server-server.

## Special Thanks

Some projects I've studied which inspired this project.

- rosedb: a kv system based on bitcask.
- minidb: a minimum implementation of bitcask, written by the same author for tutorial purpose.
- groupcache: distributed cache system using consistent hash + singleflight.
- secret project A: a distributed kv system based on storage unit consists of rocksdb + LRU cache, using hashslot to achieve distributed storage and elastic expansion.
- bitcask paper: https://riak.com/assets/bitcask-intro.pdf
- kafka: a famous, widely used industrial distributed message queue.
- distributed system design patterns: https://github.com/dreamhead/patterns-of-distributed-systems