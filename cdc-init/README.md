# LPE Change Data Capture Initialization (`cdc-init`)

This service handles the multi-step process of enabling Change Data
Capture on tables within the IFS database, reading requests from and
posting progress updates to an Apache Kafka topic.

## Overview

This service will _only_ act upon messages posted to the control topic
that have a `"status"` of `submitted`. The process it follows is:

* Create the queue (and queue table if required) to record the Change
  Data Capture messages to in the database.
* Create the trigger to record Change Data Capture messsages on any
  change to the specified table.
* Create the Apache Kafka Topic to which the messages will ultimately
  be recorded.
* Seed the topic with "insert" messages for all existing rows of the
  specified table.
* Enable the trigger to capture modifications from the point of taking
  a snapshot for seeding.

Progress updates are posted back to the same control topic from which
the requests are read. The message bodies are updated with new
`"timestamp"`s and `"status"`es, which may be one of:

* `"queue-created"`
* `"trigger-created"`
* `"topic-created"`
* `"prepared"` indicates that all of the required objects have been
  created and the topic can now be seeded.
* `"seeding"` sent multiple times during the seeding process with an
  additional `"progress"` entry added to the message body which is a
  tuple of `[records seeded, total selected]`.
* `"error"` sent in the event of any error occurring during
  processing, the error details are added under an `"error"` key.

## Usage

### Dependencies

Requires the following PL/SQL packages:

* `lpe_msg_queue_adm_api`
* `lpe_msg_queue_api`
* `lpe_change_data_capture_api`

Must be run under a user account with the following permissions:

```sql
create user <username> identified by <password>
  default tablespace <tablespace>
  quota unlimited on <tablespace>;
grant create session to <username>;
grant execute on ifsapp.lpe_queue_msg to <username>;
grant execute on ifsapp.lpe_msg_queue_adm_api to <username>;
grant execute on ifsapp.lpe_msg_queue_api to <username>;
grant execute on ifsapp.lpe_change_data_capture_api to <username>;
```

Note that, as per the `lpe_msg_queue*_api` guidelines the user should
default to creating objects in a tablespace dedicated to Change Data
Capture.

### Environment Variables

Required:

* `DB_NAME`
* `DB_SERVER`
* `DB_USER`
* `DB_PASSWORD`
* `ZOOKEEPER` the ZooKeeper connection string for the Kafka brokers.

Optional:

* `CONTROL_TOPIC` the name of the Kafka topic from which to read/post
  requests and progress updates. Will default to `change-data-capture`.

The control topic will be created if it does not exist.

### Running

In all cases you need to first establish the environment variables as
detailed above. (Note: this project uses the [environ] library so any
supported method—`ENV` vars, `.lein-env` files, etc.—will work.)

From the project directory:

    lein run

Using a compiled `.jar` file:

    java -jar <path/to/cdc-init.jar>

As a [Docker] container:

    docker run \
      -d \
      --name=cdc-init \
      -e DB_NAME=<database> \
      -e DB_SERVER=<hostname> \
      -e DB_USER=<username> \
      -e DB_PASSWORD=<password> \
      -e ZOOKEEPER=<connect string> \
      lpe/cdc-init

[environ]: https://github.com/weavejester/environ
[Docker]: https://www.docker.com/

## Building a Docker Image

Nothing special, you just need to ensure you've built the uberjar first:

    lein unberjar
    docker build -t lpe/cdc-init:latest .

## License

Copyright © 2015 Lymington Precision Engineers Co. Ltd.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
