# Installing Trellis

## Requirements

  * A [Zookeeper](http://zookeeper.apache.org) ensemble (3.5.x or later)
  * A [Kafka](http://kafka.apache.org) cluster (0.11.x or later).
  * Java 8 or 9
  * An [asynchronous processing application](https://github.com/trellis-ldp/trellis-rosid-file-streaming)

The location of Kafka and Zookeeper will be defined in the `./etc/config.yml` file.

## Installation

To install Trellis as a systemd service on linux, follow these steps:

1. Move the unpacked Trellis directory to a location such as `/opt/trellis`.
   If you choose a different location, please update the `./etc/trellis.service` script.

2. Edit the `./etc/environment` file as desired (optional).

3. Edit the `./etc/config.yml` file as desired (optional).

4. Create a trellis user:

        $ sudo useradd -r trellis -s /sbin/nologin

5. Create data directories

        $ sudo mkdir /var/lib/trellis
        $ sudo chown trellis.trellis /var/lib/trellis

6. Install the systemd file:

        $ sudo ln -s /opt/trellis/etc/trellis.service /etc/systemd/system/trellis.service

7. Reload systemd to see the changes

        $ sudo systemctl daemon-reload

8. Start the trellis service

        $ sudo systemctl start trellis

To check that trellis is running, check the URL: `http://localhost:8080`

Application health checks are available at `http://localhost:8081/healthcheck`
