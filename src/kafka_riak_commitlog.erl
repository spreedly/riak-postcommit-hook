-module(kafka_riak_commitlog).
-export([log/1]).

-define(KAFKA, [{"docker", 9092}]).
-define(TOPIC, <<"commitlog">>).
-define(PARTITION, 0).

log(Object) ->
  Bucket = extract_bucket(Object),
  Key = extract_key(Object),
  Value = extract_value(Object),
  {ok, KafkaMessage} = build_kafka_message(Bucket, Key, Value),
  produce_to_kafka(Key, KafkaMessage).

extract_bucket(Object) ->
  riak_object:bucket(Object).

extract_key(Object) ->
  riak_object:key(Object).

extract_value(Object) ->
  JsonString = riak_object:get_value(Object),
  {ok, ValueObject} = json:decode(JsonString),
  ValueObject.

build_kafka_message(Bucket, Key, Value) ->
    Message = {[
      {<<"bucket">>, Bucket},
      {<<"key">>, Key},
      {<<"value">>, Value}
    ]},
    json:encode(Message).

produce_to_kafka(Key, Message) ->
  {ok, Producer} = brod:start_link_producer(?KAFKA),
  brod:produce_sync(Producer, ?TOPIC, ?PARTITION, Key, Message).
