## Kafka Flowlet Library for CDAP


### Building the Library

The project is a multi-module maven project. It requires Maven 3.0+ and Java SDK 6+ to build.

#### Prerequisites

The library supports both Kafka 0.7.x and 0.8.x. Since artifacts for Kafka 0.7.x are not available in maven
central repository, the project includes the artifact jar of it. You have to install the artifact into local
maven repository before building the library.

    mvn initialize -P install-kafka-0.7.2

#### Build and install locally

After installing kafka-0.7.2 artifact, you can build and install all artifacts jars by:

    mvn clean install
   
or to install it without running unit-tests:

    mvn clean install -DskipTests