# SimpleKV

A simple distributed key-value storage system based on bitcask from scratch.

## Target

Here are some basic requirements:

- LRU Cache.
- An index system based on either hashmap / skiplist.
- Support string data structure. Implement GET/SET/EXPIRE/DEL method.
- Appendonly log as disk storage with Bitcask theory.
- Master-slave replication.
- Using zookeeper/etcd as service registration, leader election, etc.
- Client-side router cache and load balance.
- Using GRPC and singleflight mechanism to optimize traffic.
- Using Docker to put all stuff together.

Advanced requirements:

- Multi storage unit using hashslot / concurrency hash.
- Achieve elastic expansion.
- Frontend dashboard.
- Support list / hash / set / zset data structure.

## Special Thanks

Some projects I've studied which inspired this project.

- rosedb: a kv system based on bitcask.
- minidb: a minimum implementation of bitcask, written by the same author for tutorial purpose.
- groupcache: distributed cache system using concurrency hash + singleflight.
- secret project A: a distributed kv system based on storage unit consists of rocksdb + LRU cache, using hashslot to achieve distributed storage and elastic expansion.
- bitcask paper: https://riak.com/assets/bitcask-intro.pdf