### Latest Release (v2.0.1)
* Added support for optional shard filter parameter in DescribeStream api that allows customers to fetch child shards of a read_only parent shard.
* Fixes the [bug](https://github.com/awslabs/dynamodb-streams-kinesis-adapter/issues/62) where dynamodb-streams-kinesis-adapter was not constructing stream arn properly for china aws partition.
* Bump aws sdk to 2.32.0

### Release (v2.0.0)
* Major version upgrade to support Amazon Kinesis Client Library (KCL) version 3.x
* Upgrades AWS Java SDK to version 2.x, removing dependency on AWS SDK v1
* Provides a custom StreamsSchedulerFactory for creating a KCL scheduler optimized for DynamoDB Streams
* Support for handling multiple streams in a single KCL application
* Improved lease balancing algorithm based on sensors like CPU

### Release (v1.6.1)
* Upgrades AWS Java SDK to version 1.12.710.
* Adds dependency on Lombok 1.18.32.
* Falling back to Maven Central repository for DynamoDBLocal library.

### Release (v1.6.0)
* Upgrades Amazon Kinesis Client Library (KCL) to version 1.14.9. Customers can now use DynamoDB Streams Adapter with KCL version 1.14.9. However, DynamoDB Streams Adapter does not inherit performance optimizations like support for child shards, shard synchronization, deferred lease clean-up available in KCL.
* Fixes the [bug](https://github.com/awslabs/dynamodb-streams-kinesis-adapter/issues/40) which was causing errors in DynamoDB Streams Adapter with KCL version 1.14.0.
* With upgrade to KCL version 1.14.9, the default shard prioritization strategy has been changed to `NoOpShardPrioritization`. To retain the existing behavior, DynamoDB Streams customers should explicitly update the shard prioritization strategy to `ParentsFirstShardPrioritization` if there was no explicit override done in the application.
* Upgrades jackson-databind to version 2.12.7.1
* This release uses Apache 2.0 license.

### Release (v1.5.4)
* Upgrades AWS Java SDK to version 1.12.130
* Upgrades jackson-databind to version 2.12.6.1
* Fixes logging in `DynamoDBStreamsShardSyncer` to log only the problematic shardId instead of logging all the shardIds


### Release (v1.5.3)
* Upgrades jackson-databind to version 2.9.10.7
* Upgrades junit to version 4.13.1
* Upgrades AWS Java SDK to version 1.11.1016

### Release (v1.5.2)
* Upgrades jackson-databind to version 2.9.10.5
* Updates `StreamsWorkerFactory` to use `KinesisClientLibConfiguration` billing mode when constructing `KinesisClientLeaseManager`.

### Release (v1.5.1)
* Restores compile compatibility with KCL 1.13.3.
* Fixes a performance issue that arised when using v1.5.0 with KCL 1.12 through 1.13.2.
* Fixes a defect where `MaxLeasesForWorker` configuration was not being propagated to `StreamsLeaseTaker`.
* Finished (SHARD_END) leases will now only be delete after at least 6 hours have passed since the shard was created. This further reduces the chances of lineage replay.

### Release (v1.5.0)
* Introduces the implementation of periodic shard sync in conjunction with Amazon Kinesis Client Library v1.11.x (KCL). The default shard sync strategy is to discover new/child shards only when a consumer completes processing a shard. This default strategy constrains horizontal scaling of customer applications when consuming tables with  10,000+ partitions due to increased DescribeStream calls. Periodic shard sync guarantees that only a subset of the fleet (by default 10) will perform shard syncs, and decouples DescribeStream call volume from growth in fleet size.

* Improves inconsistency handling in DescribeStream result aggregation by fixing any parent-open-child-open cases. This ensures that shard sync does not fail due to an assertion failure in KCL on this type of inconsistency.

* Modifies finished shard lease cleanup mechanism. Leases for shards that have been completely processed are now deleted only after all their children shards have been completely processed. This will prevent shard lineage replay issues, instances of which have been reported in the past by some customers.

* Introduces `StreamsLeaseTaker` with improved load-balancing of leases among workers.
    * SHARD_END and non-SHARD_END check-pointed leases are balanced independently.
    * Leases are now stolen evenly from other workers instead of from only the most loaded worker. `MaxLeasesToStealAtOneTime` no longer needs to be specified by users. It is now determined automatically based on the number of leases held by the worker. The user-specified value for this is no longer used.

* Users should continue using factory methods from `StreamsWorkerFactory` to create KCL Worker as specified in the guidance of Release v1.4.x.
* We strongly recommended that you create only one worker per host in your processing fleet to get optimal performance from DynamoDB Streams service.

### Release (v1.4.x)
* This release fixes an issue of high propagation delay of streams records when processing streams on small tables. This issue occurs when KCL ShardSyncer is not discovering new shards due to server side delays in shard creation or in reporting new shard creation to internal services. The code is implemented in a new implementation of IKinesisProxy interface called DynamoDBStreamsProxy which is part of the latest release.
* This release requires Kinesis Client Library version >= 1.8.10. Version 1.8.10 has changes to allow IKinesisProxy injection into the KCL Worker builder which is required by DynamoDB Streams Kinesis Adapter v1.4.x for
  injection of DynamoDBStreamsProxy into the KCL worker during initialization. Please refer to [Kinesis Client Library release notes for 1.8.10](https://github.com/awslabs/amazon-kinesis-client/blob/master/CHANGELOG.md#release-1810) for more information.
* Suggested AWS Java SDK version >= 1.11.218
* It is highly recommended to [configure][kcl-configuration] Kinesis Client Library with `MaxRecords = 1000` and `IdleTimeInMillis = 500` to optimize DynamoDB Streams costs.

### Guidance for injecting DynamoDBStreamsProxy into KCL worker when using DynamoDB Streams Kinesis Adapter v1.4.x.
To fix high propagation delay problems, opt-into using DynamoDBStreamsProxy (instead of the default KinesisProxy) by using the StreamsWorkerFactory factory method (shown below). This injects an instance of DynamoDBStreamsProxy into the created KCL worker.
```
       final Worker worker = StreamsWorkerFactory
           .createDynamoDbStreamsWorker(
               recordProcessorFactory,
               workerConfig,
               adapterClient,
               amazonDynamoDB,
               amazonCloudWatchClient);
```