# Trellis Rosid Implementation

[![Build Status](https://travis-ci.org/trellis-ldp/trellis-rosid.png?branch=master)](https://travis-ci.org/trellis-ldp/trellis-rosid)
[![Build status](https://ci.appveyor.com/api/projects/status/i1geqkvi48w5y9om?svg=true)](https://ci.appveyor.com/project/acoburn/trellis-rosid)
[![Coverage Status](https://coveralls.io/repos/github/trellis-ldp/trellis-rosid/badge.svg?branch=master)](https://coveralls.io/github/trellis-ldp/trellis-rosid?branch=master)

This is an implementation of the Trellis Linked Data API, using a file-based persistence and internal Kafka event bus.

There are two parts to this code: an [HTTP layer](trellis-rosid-app) and an
[asynchronous processor](trellis-rosid-file-streaming). Installation and configuration information is available
on each subproject README page.

## Building Trellis/Rosid

1. Run `./gradlew clean install` to build the application or download one of the releases.

