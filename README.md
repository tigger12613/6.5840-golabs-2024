# MIT 6.824 Distributed Systems Labs
## [MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
* MapReduce simplified data processing on large clusters
* implement at /src/mr
* `coordinator.go`
    * master of mapreduce, designate task to workers.
* `worker.go`
    * require task from master.
* failure process
    * Single failure if master` fail task failed
    * if worker fail or slow, master can give task to another worker.

## [Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)
* Raft is a consensus algorithm for managing a replicated log.
* implement at `/src/raft`
* implement features like
  * leader election
  * log replicate
  * data persistent
  * log compaction(snapshot)
* Achieve fault tolerance, consistency and linearizability

## [Fault-tolerant Key/Value Service](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html)
* Build a fault-tolerant key/value storage service using my Raft library.
* key/value service will be a replicated state machine, consisting of several key/value servers that each maintain a database of key/value pairs.
* implement at `/src/kvraft`

## [Sharded Key/Value Service](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)
* Build a key/value storage system that "shards," or partitions, the keys over a set of replica groups. A shard is a subset of the key/value pairs.
* Build a shard controller to manage a sequence of numbered configurations. Each configuration describes a set of replica groups and an assignment of shards to replica groups.
* implement Sharded Key/Value Service at `/src/shardkv`
* implement shard controller at `/src/shardctrler`
