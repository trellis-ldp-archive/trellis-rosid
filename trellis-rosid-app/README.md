# Trellis Application

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.trellisldp/trellis-app/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.trellisldp/trellis-app/)

## Requirements

  * A [Zookeeper](http://zookeeper.apache.org) ensemble (3.5.x or later)
  * A [Kafka](http://kafka.apache.org) cluster (0.11.x or later)
  * Java 8 or 9
  * An [asynchronous processing application](https://github.com/trellis-ldp/trellis-rosid-file-streaming)

The location of Kafka and Zookeeper will be defined in the `./etc/config.yml` file.

## Running Trellis

Unpack a zip or tar distribution. In that directory, modify `./etc/config.yml` to match the
desired values for your system.

To run trellis directly from within a console, issue this command:

```bash
$ ./bin/trellis-app server ./etc/config.yml
```

**Note**: When running trellis, please be sure to also have an active
[asynchronous processor](https://github.com/trellis-ldp/trellis-rosid-file-streaming).

## Installation

To install Trellis as a [`systemd`](https://en.wikipedia.org/wiki/Systemd) service on linux,
follow the steps below. `systemd` is used by linux distributions such as CentOS/RHEL 7+ and Ubuntu 15+.

1. Move the unpacked Trellis directory to a location such as `/opt/trellis`.
   If you choose a different location, please update the `./etc/trellis.service` script.

2. Edit the `./etc/environment` file as desired (optional).

3. Edit the `./etc/config.yml` file as desired (optional).

4. Create a trellis user:

```bash
$ sudo useradd -r trellis -s /sbin/nologin
```

5. Create data directories. A different location can be used, but then please update
   the `./etc/config.yml` file.

```bash
$ sudo mkdir /var/lib/trellis
$ sudo chown trellis.trellis /var/lib/trellis
```

6. Install the systemd file:

```bash
$ sudo ln -s /opt/trellis/etc/trellis.service /etc/systemd/system/trellis.service
```

7. Reload systemd to see the changes

```bash
$ sudo systemctl daemon-reload
```

8. Start the trellis service

```bash
$ sudo systemctl start trellis
```

To check that trellis is running, check the URL: `http://localhost:8080`

Application health checks are available at `http://localhost:8081/healthcheck`

## Building Trellis

1. Run `./gradlew clean install` to build the application or download one of the releases.
2. Unpack the appropriate distribution in `./build/distributions`
3. Start the application according to the steps above

## Configuration

The web application wrapper (Dropwizard.io) makes many [configuration options](http://www.dropwizard.io/1.2.0/docs/manual/configuration.html)
available. Any of the configuration options defined by Dropwizard can be part of your application's configuration file.

Trellis defines its own configuration options, including:

```yaml
binaries:
  path: /path/to/binaries
```

| Name | Default | Description |
| ---- | ------- | ----------- |
| path | (none) | The path for storing binaries |

```yaml
resources:
  path: /path/to/resources
```

| Name | Default | Description |
| ---- | ------- | ----------- |
| path | (none) | The path for storing resources |

```yaml
baseUrl: http://localhost:8080/
```

| Name | Default | Description |
| ---- | ------- | ----------- |
| baseUrl | (none) | A defined baseUrl for resources in this partition. If not defined, the `Host` request header will be used |

```yaml
namespaces:
    file: /path/to/namespaces.json
```

| Name | Default | Description |
| ---- | ------- | ----------- |
| file | (none) | The path to a JSON file defining namespace prefixes |

```yaml
zookeeper:
    ensembleServers: localhost:2181
```

| Name | Default | Description |
| ---- | ------- | ----------- |
| ensembleServers | (none) | The location of the zookeeper ensemble servers (comma separated) |

```yaml
kafka:
    bootstrapServers: localhost:9092
```

| Name | Default | Description |
| ---- | ------- | ----------- |
| bootstrapServers | (none) | The location of the kafka servers (comma separated) |

```yaml
auth:
    webac:
        enabled: true
    anon:
        enabled: true
    jwt:
        enabled: true
        base64Encoded: false
        key: secret
    basic:
        enabled: true
        usersFile: /path/to/users.auth
```

| Name | Default | Description |
| ---- | ------- | ----------- |
| webac / enabled | true | Whether WebAC authorization is enabled |
| anon / enabled | false | Whether anonymous authentication is enabled |
| jwt / enabled | true | Whether jwt authentication is enabled |
| jwt / base64Encoded | false | Whether the key is base64 encoded |
| jwt / key | (none) | The signing key for JWT tokens |
| basic / enabled | true | Whether basic authentication is enabled |
| basic / usersFile | (none) | The path to a file where user credentials are stored |

```yaml
cors:
    enabled: true
    allowOrigin:
        - "*"
    allowMethods:
        - "GET"
        - "POST"
        - "PATCH"
    allowHeaders:
        - "Content-Type"
        - "Link"
    exposeHeaders:
        - "Link"
        - "Location"
    maxAge: 180
    allowCredentials: true
```

| Name | Default | Description |
| ---- | ------- | ----------- |
| enabled | false | Whether [CORS](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS) is enabled |
| allowOrigin | "*" | A list of allowed origins |
| allowMethods | "PUT", "DELETE", "PATCH", "GET", "HEAD", "OPTIONS", "POST" | A list of allowed methods |
| allowHeaders | "Content-Type", "Link", "Accept", "Accept-Datetime", "Prefer", "Want-Digest", "Slug", "Digest" | A list of allowed request headers |
| exposeHeaders | "Content-Type", "Link", "Memento-Datetime", "Preference-Applied", "Location", "Accept-Patch", "Accept-Post", "Digest", "Accept-Ranges", "ETag", "Vary" | A list of allowed response headers |
| maxAge | 180 | The maximum age (in seconds) of pre-flight messages |
| allowCredentials | true | Whether the actual request can be made with credentials |

```yaml
async: false
```

| Name | Default | Description |
| ---- | ------- | ----------- |
| async | false | Set this to `true` if resource caches should be generated by an async processor; otherwise they will be generated synchronously. **Note**: setting this to `true` will make write operations faster for clients, but the availability of the updated content will appear to lag (because it is generated asynchronously). |

```yaml
cacheMaxAge: 86400
```

| Name | Default | Description |
| ---- | ------- | ----------- |
| cacheMaxAge | 86400 | The value of the `Cache-Control: max-age=` response header |

