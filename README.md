# SimpleKV

A simple distributed key-value storage system based on bitcask from scratch.

## Roadmap

Progress: 🟦🟦🟦🟦🟦◻️◻️◻️◻️◻️

Here are some basic requirements:

- [x] LRU Cache.
- An index system based on either hashmap / skiplist.
  - [x] hashmap
  - [ ] skiplist
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
- [ ] Using Docker to put all stuff together.

Advanced requirements:

- [ ] Multi storage unit using hashslot / consistent hash.
- [ ] Achieve elastic expansion.
- [ ] Frontend dashboard.
- [ ] Support list / hash / set / zset data structure.

## Special Thanks

Some projects I've studied which inspired this project.

- rosedb: a kv system based on bitcask.
- minidb: a minimum implementation of bitcask, written by the same author for tutorial purpose.
- groupcache: distributed cache system using consistent hash + singleflight.
- secret project A: a distributed kv system based on storage unit consists of rocksdb + LRU cache, using hashslot to achieve distributed storage and elastic expansion.
- bitcask paper: https://riak.com/assets/bitcask-intro.pdf
- kafka: a famous, widely used industrial distributed message queue.
- distributed system design patterns: https://github.com/dreamhead/patterns-of-distributed-systems