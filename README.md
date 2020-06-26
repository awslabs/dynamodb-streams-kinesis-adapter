# DynamoDB Streams Kinesis Adapter for Java

**DynamoDB Streams Kinesis Adapter** implements the Amazon Kinesis interface so that your application can use KCL to consume and process data from a DynamoDB stream. You can get started in minutes using ***Maven***.

* [DynamoDB Streams Developer Guide][docs-dynamodb-streams]
* [Amazon Kinesis Client Library GitHub and Documentation][docs-kcl]
* [DynamoDB Forum][dynamodb-forum]
* [DynamoDB Details][dynamodb-details]
* [Issues][adapter-issues]

## Features

* The DynamoDB Streams Kinesis Adapter for Amazon Kinesis Client Library (KCL) is the best way to ingest and process data records from DynamoDB Streams.
* The KCL is designed to process streams from Amazon Kinesis, but by adding the DynamoDB Streams Kinesis Adapter, your application can process DynamoDB Streams instead, seamlessly and efficiently.

## Release Notes
### Latest Release (v1.5.2)
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

## Getting Started

1. **Sign up for AWS** - Before you begin, you need an AWS account. Please see the [AWS Account and Credentials][docs-signup] section of the developer guide for information about how to create an AWS account and retrieve your AWS credentials. You don’t need this if you’re using DynamoDB Local.
1. **Minimum requirements** - To run the SDK you will need **Java 1.8+**. For more information about the requirements and optimum settings for the SDK, please see the [Java Development Environment][docs-signup] section of the developer guide.
1. **Install the DynamoDB Streams Kinesis Adapter** - Using ***Maven*** is the recommended way to install the DynamoDB Streams Kinesis Adapter and its dependencies, including the AWS SDK for Java.  To download the code from GitHub, simply clone the repository by typing: `git clone https://github.com/awslabs/dynamodb-streams-kinesis-adapter.git`, and run the Maven command described below in "Building From Source". You may also depend on the maven artifact [com.amazonaws:dynamodb-streams-kinesis-adapter][adapter-maven].
1. **Build your first application** - There is a walkthrough to help you build first application using this adapter. Please see [Using the DynamoDB Streams Kinesis Adapter to Process Stream Records][docs-adapter].

## Including as a Maven dependency

Add the following to your Maven pom file:
```
<dependency>
    <groupId>com.amazonaws</groupId>
    <artifactId>dynamodb-streams-kinesis-adapter</artifactId>
    <version>1.5.1</version>
</dependency>
```

## Building From Source

Once you check out the code from GitHub, you can build it using Maven: `mvn clean install`

[adapter-issues]: https://github.com/awslabs/dynamodb-streams-kinesis-adapter/issues
[adapter-maven]: http://mvnrepository.com/artifact/com.amazonaws/dynamodb-streams-kinesis-adapter
[dynamodb-details]: https://aws.amazon.com/dynamodb
[dynamodb-forum]: https://developer.amazonwebservices.com/connect/forum.jspa?forumID=131
[docs-adapter]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.KCLAdapter.html
[docs-dynamodb-streams]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
[docs-kcl]: https://github.com/awslabs/amazon-kinesis-client
[docs-signup]: https://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html
[kcl-configuration]: https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java
