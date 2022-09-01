FROM maven:3-openjdk-8-slim AS BUILD_CONNECT_TRANSFORM_ARCHIVE_PLUGIN
WORKDIR /tmp
RUN apt-get update && apt-get install -y git
COPY . /tmp/kafka-connect-transform-archive
RUN cd kafka-connect-transform-archive && mvn package
