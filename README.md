# LPE Change Data Capture

This project encompasses the background services required to enable
Change Data Capture within our IFS application database and transfer
the change messages to Apache Kafka for general availability and
longer term storage.

## Overview

Change Data Capture is the process of recording all insert, update,
and delete events within a RDBMS, in our case that is the Oracle based
IFS database.

The _purpose_ of such a system is to enable stream processing of data
modification events. Some use cases are:

* Keep a detailed history/record changes for auditing/logging.
* Provide a central point from which to subscribe to events arising
  from changing data.
* Perform real time processing of business data.

Capturing these events turns them into a universal pipeline of data—a
global commit log—which can be consumed and processed in an unlimited
number of ways. See the references at the end of this document for
further reading on the subject.

We use [Apache Kafka][kafka] as the backend/datastore for this event
data as it is extremely efficient and eminently well suited to the
task.

[kafka]: http://kafka.apache.org/

## Components

There are two primary services:

* [`cdc-init`](cdc-init/)

  Is responsible for initializing Change Data capture on database
  tables, creating the queues and topics for message publication, and
  seeding the Kafka topics with initial record collections.
* [`cdc-publisher`](cdc-publisher/)

  Is responsible for the ongoing publication of change messages from
  the database to Kafka.

## Control/Configuration

Control of the change data capture system is accomplished by posting
messages to an Apache Kafka topic that define the actions that should
be undertaken. The topic is used as a state machine detailing the
configuration of the system. Each message consists of the following
details (stored as a JSON encoded string):

```json
{
  "table": "ifsapp.shop_ord_tab",
  "trigger": "ifsapp.lpe_cdc_shop_ord",
  "queue": "changedata.lpe_cdc_shop_ord",
  "queue-table": "changedata.shop_ord",
  "status": "submitted",
  "timestamp": "20151113T132903.564Z"
}
```

The table name is used as the message key so that the can be
partitioned (if required) and reduced by key down to their latest
state.

Within the message body the `"table"`, `"trigger"`, `"queue"`, and
`"queue-table"` entries specify the schema qualified name of the
corresponding object used for change data capture.

`"table"` is the table from which changes are captured; `"trigger"`,
the name of the capture trigger; `"queue"`, the Oracle Advanced Queue
to which the changes are posted before being read and published to
Kafka; and `"queue-table"` the backing table for that queue.

Note that `"table"` and `"trigger"` will be in the IFS application
owner schema whereas `"queue"` and `"queue-table"` should be in the
default schema of the user under which the service is running.

`"trigger"` names (excluding the schema) must be 30 characters or
shorter. `"queue"` and `"queue-table"` names must be 24 characters or
shorter.

Each message will also contain a ISO8601 `"timestamp"` string and a
`"status"` which can be one of:

* `"submitted"` which indicates that the table has been submitted to
  the Change Data Capture system, to be initialized and then monitored
  and have its changes published.
* `"active"` indicates that the table is actively being monitored and
  its changes published to Kafka.
* `"error"` which is recorded is there is an error establishing Change
  Data Capture of occurring during operation that cannot be recovered
  from. The error details are given under an `"error"` field.

There are several other intermediate states:

* `"seeding"` which indicates that the topic is being seeded with the
  details of records currently in the table. Each such message will
  have an additional `"progress"` entry which is a tuple giving the
  `[number of records processed, total number in table]`.
* `"trigger-created"`, `"queue-created"`, and `"topic-created"` detail
  that the corresponding action has taken place (and when, via the
  timestamp.)
* `"prepared"` logs the time at which the requisite
  trigger/queue/topic were ready and seeding could be started.

Refer to the individual sub-projects for further details/more specific
information.

## References

Pretty much anything [Martin Kleppmann][mkleppmann] has written or
[Confluent] have posted to their blog. The following articles were
particularly inspiring:

* [Turning the Database Inside Out](http://www.confluent.io/blog/turning-the-database-inside-out-with-apache-samza/)
* [Putting Apache Kafka To Use](http://www.confluent.io/blog/stream-data-platform-1/)
* [Using Logs to Build a Solid Data Infrastructure](http://www.confluent.io/blog/using-logs-to-build-a-solid-data-infrastructure-or-why-dual-writes-are-a-bad-idea/)
* [Bottled Water: Real-Time Integration of PostgreSQL and Kafka](http://martin.kleppmann.com/2015/04/23/bottled-water-real-time-postgresql-kafka.html)

[mkleppmann]: http://martin.kleppmann.com/
[Confluent]: http://www.confluent.io/

## License

Copyright © 2015 Lymington Precision Engineers Co. Ltd.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
