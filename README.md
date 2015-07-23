# DynamoDB Streams Kinesis Adapter for Java

**DynamoDB Streams Kinesis Adapter** implements the Amazon Kinesis interface so that your application can use KCL to consume and process data from a DynamoDB stream. You can get started in minutes using ***Maven***.

* [DynamoDB Developer Guide][docs-dynamodb-streams]
* [Amazon Kinesis Client Library GitHub and Documentation][docs-kcl]
* [DynamoDB Forum][dynamodb-forum]
* [DynamoDB Details][dynamodb-details]
* [Issues][adapter-issues]

## Features

* The DynamoDB Streams Kinesis Adapter for Amazon Kinesis Client Library (KCL) is the best way to ingest and process data records from DynamoDB Streams.
* The KCL is designed to process streams from Amazon Kinesis, but by adding the DynamoDB Streams Kinesis Adapter, your application can process DynamoDB Streams instead, seamlessly and efficiently.

## Release Notes
This is a preview release of this adapter that works with the preview release of DynamoDB Streams. To learn more about the preview release, please see the [DynamoDB Developer Guide][docs-dynamodb-streams].

## Getting Started

1. **Sign up for AWS** - Before you begin, you need an AWS account. Please see the [AWS Account and Credentials][docs-signup] section of the developer guide for information about how to create an AWS account and retrieve your AWS credentials. You don’t need this if you’re using DynamoDB Local.  In fact unless you’re signed up for preview, you can ONLY use this with local.
1. **Minimum requirements** - To run the SDK you will need **Java 1.7+**. For more information about the requirements and optimum settings for the SDK, please see the [Java Development Environment][docs-signup] section of the developer guide.
1. **Install the preview release of the AWS Java SDK** - To download the SDK please follow the [AWS Java SDK Preview][sdk]. Then you need to install AWS JAVA SDK jar into your local Maven repository by typing: `mvn install:install-file -Dfile=aws-java-sdk-1.9.4a-preview.jar -DgroupId=com.amazonaws -DartifactId=aws-java-sdk -Dversion=1.9.4a-preview -Dpackaging=jar`.
1. **Install the DynamoDB Streams Kinesis Adapter** - Using ***Maven*** is the recommended way to install the DynamoDB Streams Kinesis Adapter and its dependencies, including the AWS SDK for Java.  To download the code from GitHub, simply clone the repository by typing: `git clone https://github.com/awslabs/dynamodb-streams-kinesis-adapter`, and run the Maven command described below in "Building From Source".
1. **Build your first application** - There is a walkthrough to help you build first application using this adapter. Please see [DynamoDB Developer Guide][docs-dynamodb-streams].

## Building From Source

Once you check out the code from GitHub, you can build it using Maven.  To disable the GPG-signing in the build, use: `mvn clean install -Dgpg.skip=true`

[docs-dynamodb-streams]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
[docs-kcl]: https://github.com/awslabs/amazon-kinesis-client
[dynamodb-forum]: http://developer.amazonwebservices.com/connect/forum.jspa?forumID=131
[dynamodb-details]: http://aws.amazon.com/dynamodb
[adapter-issues]: https://github.com/awslabs/dynamodb-streams-kinesis-adapter/issues
[sdk]: http://dynamodb-preview.s3-website-us-west-2.amazonaws.com/aws-java-sdk-latest-preview.zip
[docs-signup]: http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html
