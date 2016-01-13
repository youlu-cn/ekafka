# ekafka

A simple Kafka client written in Erlang.

## Features

* support configuration from zookeeper or Kafka brokers
* auto reconnect to new broker when old one down

### Producer

* producer do NOT need to care the partitions, just focus to produce

### Consumer

* support to load the group offset that consumed last time
* support consumer offset auto commit to zookeeper or Kafka group coordinator broker


## Usage

### Add a topic

- for producer, call

> ekafka:add_producer/1

- for consumer client, call

> ekafka:add_consumer/2

Both arguments need a topic name. Note that this operation is async, and if topic name is invalid, ekafka will down.
If you are not sure about the topic name, call

> ekafka_util:to_topic/1

which will convert the input string to a valid topic name.

### Produce

To produce any message to Kafka, just call

> ekafka:produce/2

Returns

> {error, no_topic} means the topic does not exist.
> {error, server_down} means the lead broker has changed, you can re-call produce immediately which will be balanced to other partitions
> {error, server_error} is an Kafka server error, and please check logs for detail
> ok

### Consume

If you don't care the partition, just call

> ekafka:consume/1

This call will be blocked until received messages from any one partition.

The following function can get the partition list for specified topic

> ekafka:get_partition_list/1

And you can call

> ekafka:consume/2

to consume messages from a specified topic. This function will be blocked until messages received also.

### TODO:

More un-blocking API will be added later.

## Configurations

{ekafka, [{conf, [

    {brokers, [{1, {{10,142,99,87},9092}}, {2, {{10,142,99,88},9092}}, {3, {{10,142,99,89}, 9092}}]},
    %%{zookeeper, [{{10,142,99,87},2181,30000,10000}, {{10,142,99,88},2182,30000,10000}, {{10,142,99,89},2183,30000,10000}]},
    %% for producer
    {produce_workers, 8},
    {wait_all_servers, true},
    {hash_partition_by_key, false},
    %% for consumer
    {auto_commit_timeout, 30000},
    {consume_block_timeout, 500},
    {consume_from_beginning, true},
    {max_message_bytes, 1048576}

  ]}
]}

- If zookeeper is specified, the topic metadata will be retrieved from zookeeper. If topic is not exist, a random broker will be called to create it.
- If zookeeper is not set, brokers must be in configuration. Or we cannot work.
- produce_workers can set max worker process count for each partition of a producer, default is 8.
- wait_all_servers means if we need to wait all Kafka replicas when producing, default is false.
- hash_partition_by_key can hash the message to a specified partition when producing, default is false.
- auto_commit_timeout
- consume_block_timeout, Kafka server blocking time when consuming.
- consume_from_beginning, when consumed offset cannot be found from Kafka or zookeeper, which offset should be started to consume, default is from beginning.
- max_message_bytes, max message bytes for each partition when consuming.
