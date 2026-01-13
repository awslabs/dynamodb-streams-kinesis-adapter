## DynamoDB Streams Kinesis Adapter for Java
DynamoDB Streams Kinesis Adapter implements the Amazon Kinesis interface so that your application can use Amazon Kinesis Client Library \(KCL\) to consume and process data from a DynamoDB stream. You can get started in minutes using *Maven*.

• [DynamoDB Streams Developer Guide][1]

• [Amazon Kinesis Client Library GitHub and Documentation][2]

• [DynamoDB Forum][3]

• [DynamoDB Details][4]

• [Issues][5]

## Features
• The DynamoDB Streams Kinesis Adapter for KCL is the best way to ingest and process data records from DynamoDB Streams.

• KCL is designed to process streams from Amazon Kinesis, but by adding the DynamoDB Streams Kinesis Adapter, your application can process DynamoDB Streams instead, seamlessly and efficiently.

• Version 2.x brings compatibility with KCL version 3, enabling modern stream processing capabilities with enhanced performance and reliability.

## Getting Started
• **Sign up for AWS** - Before you begin, you need an AWS account. Please see the [AWS Account and Credentials][6] section of the developer guide for information about how to create an AWS account and retrieve your AWS credentials. You don't
need this if you're using DynamoDB Local.

• **Minimum requirements** - To run the SDK you will need Java 8+. For more information about the requirements and optimum settings for the SDK, please see the [Java SDKv2 documentation][7]

• **Install the DynamoDB Streams Kinesis Adapter** - Using _Maven_ is the recommended way to install the DynamoDB Streams Kinesis Adapter and its dependencies, including the AWS SDK for Java. To download the code from GitHub, simply clone the
repository by running: git clone https://github.com/awslabs/dynamodb-streams-kinesis-adapter.git, and run the Maven command described below in "Building From Source". You may also depend on the maven artifact [com.amazonaws:dynamodb-streams-
kinesis-adapter][8].

• **Build your first application** - There is a walkthrough to help you build first application using this adapter. Please see [Using the DynamoDB Streams Kinesis Adapter to Process Stream Records][9].

## Including as a Maven dependency
Add the following to your Maven pom file:

```
<dependency>
   <groupId>com.amazonaws</groupId>
   <artifactId>dynamodb-streams-kinesis-adapter</artifactId>
   <version>2.1.0</version>
</dependency>
```

## Migration from v1.x to v2.x
Follow the migration guide provided in [Migrating from KCL 1.x to KCL 3.x](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/streams-migrating-kcl.html)

## Building From Source
Once you check out the code from GitHub, you can build it using Maven: mvn clean install

## Release Notes

See [CHANGELOG.md](CHANGELOG.md)


[1]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
[2]: https://github.com/awslabs/amazon-kinesis-client
[3]: https://developer.amazonwebservices.com/connect/forum.jspa?forumID=131
[4]: https://aws.amazon.com/dynamodb
[5]: https://github.com/awslabs/dynamodb-streams-kinesis-adapter/issues
[6]: https://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html
[7]: https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html
[8]: http://mvnrepository.com/artifact/com.amazonaws/dynamodb-streams-kinesis-adapter
[9]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.KCLAdapter.html