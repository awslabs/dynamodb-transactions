# Transactions for Amazon DynamoDB

**_[IMPORTANT]_ Since November 2018, DynamoDB offers transactional APIs, simplifying the developer experience of making coordinated, all-or-nothing changes to multiple items both within and across tables. DynamoDB Transactions provide atomicity, consistency, isolation, and durability (ACID) in DynamoDB, enabling you to maintain data correctness in your applications more easily. We strongly recommend all developers to use DynamoDB’s built-in, servers-side transactions instead of this client-side library. To learn more about DynamoDB Transactions, see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transactions.html.**
 

**Amazon DynamoDB Client-Side Transactions** enables Java developers to easily perform atomic writes and isolated reads across multiple items and tables when building high scale applications on [Amazon DynamoDB][dynamodb]. You can get started in minutes using ***Maven***.

* [Transactions Details & Design][design]
* [DynamoDB Forum][sdk-forum]
* [Transactions Library Issues][sdk-issues] 

The **Amazon DynamoDB Client-Side Transactions** library is built on top of the low-level Amazon DynamoDB client in the AWS SDK for Java.  For support in using and installing the AWS SDK for Java, see:

* [API Docs][docs-api]
* [SDK Developer Guide][docs-guide]
* [AWS SDK Forum][sdk-forum]
* [SDK Homepage][sdk-website]
* [Java Development AWS Blog][sdk-blog]

## Features

* **Atomic writes:** Write operations to multiple items are either all go through, or none go through.
* **Isolated reads:** Read operations to multiple items are not interfered with by other transactions.
* **Sweepers:** In-flight transaction state is stored in a separate table, and convenience methods are provided to "sweep" this table for "stuck" transactions.   
* **Easy to use:** Mimics the Amazon DynamoDB API by using the request and response objects from the low-level APIs, including PutItem, UpdateItem, DeleteItem, and GetItem.
* **Table helpers:** Includes useful methods for creating tables such as and waiting for them to become ACTIVE.

## Getting Started

1. **Sign up for AWS** - Before you begin, you need an AWS account. Please see the [AWS Account and Credentials][docs-signup] section of the developer guide for information about how to create an AWS account and retrieve your AWS credentials.
1. **Minimum requirements** - To run the SDK you will need **Java 1.6+**. For more information about the requirements and optimum settings for the SDK, please see the [Java Development Environment][docs-signup] section of the developer guide.
1. **Install the Amazon DynamoDB Transactions Library** - Using ***Maven*** is the recommended way to install the Amazon DynamoDB Transactions Library and its dependencies, including the AWS SDK for Java.  To download the code from GitHub, simply clone the repository by typing: `git clone https://github.com/awslabs/dynamodb-transactions`, and run the Maven command described below in "Building From Source".
1. **Run the examples** - The included *TransactionExamples* automatically creates the necessary transactions tables, an example table for data and executes several operations with transactions.  You can run the examples using Maven by:
  1.  Ensure you have already built the library using Maven (see "Building From Source" below)
  2.  Change into the *examples* directory of the project
  2.  Add your AWS Credentials to the file: *src/main/resources/com/amazonaws/services/dynamodbv2/transactions/examples/AwsCredentials.properties*
  3.  Compile the subproject by typing: `mvn clean install`
  4.  Run the examples by typing: `mvn exec:java -Dexec.mainClass="com.amazonaws.services.dynamodbv2.transactions.examples.TransactionExamples"` 

## Building From Source

Once you check out the code from GitHub, you can build it using Maven.  To disable the GPG-signing in the build, use: `mvn clean install -Dgpg.skip=true`

[design]: https://github.com/awslabs/dynamodb-transactions/blob/master/DESIGN.md
[sdk-install-jar]: http://sdk-for-java.amazonwebservices.com/latest/aws-java-sdk.zip
[aws]: http://aws.amazon.com/
[dynamodb]: http://aws.amazon.com/dynamodb
[dynamodb-forum]: https://forums.aws.amazon.com/forum.jspa?forumID=131
[sdk-website]: http://aws.amazon.com/sdkforjava
[sdk-forum]: http://developer.amazonwebservices.com/connect/forum.jspa?forumID=70
[sdk-blog]: https://java.awsblog.com/
[sdk-issues]: https://github.com/awslabs/dynamodb-transactions/issues
[sdk-license]: http://www.apache.org/licenses/LICENSE-2.0
[docs-api]: http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html
[docs-dynamodb-api]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/Welcome.html
[docs-dynamodb]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide
[docs-signup]: http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html
[aws-iam-credentials]: http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-roles.html
[docs-guide]: http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/welcome.html
