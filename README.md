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
* Requires Kinesis Client Library version >= 1.7.5.
* Requires AWS Java SDK version >= 1.11.115
* It is highly recommended to [configure][kcl-configuration] Kinesis Client Library with `MaxRecords = 1000` and `IdleTimeInMillis = 500` to optimize DynamoDB Streams costs.

## Getting Started

1. **Sign up for AWS** - Before you begin, you need an AWS account. Please see the [AWS Account and Credentials][docs-signup] section of the developer guide for information about how to create an AWS account and retrieve your AWS credentials. You don’t need this if you’re using DynamoDB Local.
1. **Minimum requirements** - To run the SDK you will need **Java 1.7+**. For more information about the requirements and optimum settings for the SDK, please see the [Java Development Environment][docs-signup] section of the developer guide.
1. **Install the DynamoDB Streams Kinesis Adapter** - Using ***Maven*** is the recommended way to install the DynamoDB Streams Kinesis Adapter and its dependencies, including the AWS SDK for Java.  To download the code from GitHub, simply clone the repository by typing: `git clone https://github.com/awslabs/dynamodb-streams-kinesis-adapter.git`, and run the Maven command described below in "Building From Source". You may also depend on the maven artifact [com.amazonaws:dynamodb-streams-kinesis-adapter][adapter-maven].
1. **Build your first application** - There is a walkthrough to help you build first application using this adapter. Please see [Using the DynamoDB Streams Kinesis Adapter to Process Stream Records][docs-adapter].

## Including as a Maven dependency

Add the following to your Maven pom file:
```
<dependency>
    <groupId>com.amazonaws</groupId>
    <artifactId>dynamodb-streams-kinesis-adapter</artifactId>
    <version>1.2.1-SNAPSHOT</version>
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
