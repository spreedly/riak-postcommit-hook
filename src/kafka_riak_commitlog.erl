-module(kafka_riak_commitlog).
-export([log/1, produce_to_kafka/2]).

-define(TOPIC, <<"commitlog">>).
-define(PARTITION, 0).
-define(CLIENTNAME, brod_client_1).

log(Object) ->
  Key = get_key(Object),
  Bucket = get_bucket(Object),
  try
    {ok, Key, Message} = build_kafka_message(Object),
    {Timing, ok} = timer:tc(?MODULE, produce_to_kafka, [Key, Message]),
    error_logger:info_msg("~p,~p,~p,~p", [Bucket,Key,ok,Timing])
  catch
    Type:Exception ->
      error_logger:error_msg("~p,~p,~p,~p", [Bucket,Key,Type,Exception]),
      throw(Exception)
  end.

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
  brod:produce_sync(?CLIENTNAME, ?TOPIC, ?PARTITION, Key, Message).

log_encoding_error(Object, Error) ->
  error_logger:error_msg("[kafka_riak_commitlog] Could not JSON Encode the Kafka message (Error: ~p) for the Riak object: ~p", [Error, Object]).
