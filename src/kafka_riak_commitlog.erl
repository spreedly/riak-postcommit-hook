-module(kafka_riak_commitlog).
-export([log/1]).

-define(KAFKA, [{"docker", 9092}]).
-define(TOPIC, <<"commitlog">>).
-define(PARTITION, 0).

log(Object) ->
  {ok, Key, Message} = build_kafka_message(Object),
  produce_to_kafka(Key, Message).

build_kafka_message(Object) ->
    Key = get_key(Object),
    Message = {[
      {<<"action">>, get_action(Object)},
      {<<"bucket">>, get_bucket(Object)},
      {<<"key">>,    Key},
      {<<"value">>,  get_value(Object)}
    ]},
    case json:encode(Message) of
      {ok, EncodedMessage} -> {ok, Key, EncodedMessage};
      {error, Error} ->
        log_encoding_error(Object, Error),
        {error, Error}
    end.

get_action(Object) ->
  Metadata = riak_object:get_metadata(Object),
  case dict:find(<<"X-Riak-Deleted">>, Metadata) of
    {ok, "true"} -> delete;
    _ -> store
  end.

get_bucket(Object) ->
  riak_object:bucket(Object).

get_key(Object) ->
  riak_object:key(Object).

get_value(Object) ->
  riak_object:get_value(Object).

produce_to_kafka(Key, Message) ->
  {ok, ClientPid} = brod:start_link_client(?KAFKA),
  brod:produce_sync(ClientPid, ?TOPIC, ?PARTITION, Key, Message).

log_encoding_error(Object, Error) ->
  error_logger:error_msg("[kafka_riak_commitlog] Could not JSON Encode the Kafka message (Error: ~p) for the Riak object: ~p", [Error, Object]).
