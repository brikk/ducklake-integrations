# Ducklake JVM projects

## Trino-Ducklake plugin

[README](trino-ducklake/README.md)

## Start

Install [SDKman](https://sdkman.io/install/) or manually install Java 25.

```shell
sdk env
```

if any errors, install the tools

```shell
sdk env install
```

Docker or Podman is required to run tests, since they use Test Containers.

## Build

```shell
./gradlew build
```