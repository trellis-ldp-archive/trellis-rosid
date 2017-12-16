# trellis-rosid-common

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.trellisldp/trellis-rosid-common/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.trellisldp/trellis-rosid-common/)

Common classes for Rosid-based implementations of the Trellis API. Rosid is based on a Kafka event bus and a
(potentially distributed) common data store.

The basic principle behind this implementation is to represent resource state as a stream of (re-playable) operations.

## Building

This code requires Java 8 and can be built with Gradle:

    ./gradlew install
