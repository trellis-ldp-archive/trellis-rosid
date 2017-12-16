# trellis-rosid-file

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.trellisldp/trellis-rosid-file/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.trellisldp/trellis-rosid-file/)

A file-based implementation of the Trellis API, based on Kafka and a distributed data store.

The basic principle behind this implementation is to represent resource state as a stream of (re-playable) operations.

## Building

This code requires Java 8 and can be built with Gradle:

    ./gradlew install
