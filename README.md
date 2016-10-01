# hdinsight-spark-livy-client
Java client for submitting a remote job to HDInsight Spark cluster.

## Introduction
The client supports to send a batch job and create an interactive session with Spark. The implmentation is based on the specification described in the following documents:
* HDInsight Remote Job Submission for Spark REST API Reference
  (https://msdn.microsoft.com/en-us/library/azure/mt613023.aspx)
* hdinsight/livy (https://github.com/hdinsight/livy)

## Build Prerequisites
You will need to download and install the following to build the code:
* JDK 1.7 or later
* Maven 3.x

### Dependency
The client code uses json-simple to parse the result from livy. The following needs to be included in your pom.xml in your maven project:
```xml
    <dependency>
    	<groupId>com.googlecode.json-simple</groupId>
    	<artifactId>json-simple</artifactId>
    	<version>1.1.1</version>
    </dependency>
```
## Sample
The sample codes are included in the src/main/java/sample directory. The sample shows you how to use the client for both batch and interactive.
