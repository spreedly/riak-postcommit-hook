-module(kafka_riak_commitlog).
-export([log/1, produce_to_kafka/2]).

-define(TOPIC, <<"commitlog">>).
-define(PARTITION, 0).
-define(CLIENTNAME, brod_client_1).

log(Object) ->
  Key = get_key(Object),
  Bucket = get_bucket(Object),
  try
    {ok, Message} = build_kafka_message(Bucket, Key, Object),
    {Timing, ok} = timer:tc(?MODULE, produce_to_kafka, [Key, Message]),
    log_output(Bucket, Key, ok, Timing)
  catch
    Type:Exception ->
      log_output(Bucket, Key, Type, Exception),
      throw(Exception)
  end.

build_kafka_message(Bucket, Key, Object) ->
    Message = {[
      {<<"action">>, get_action(Object)},
      {<<"bucket">>, Bucket},
      {<<"key">>,    Key},
      {<<"value">>,  get_value(Object)}
    ]},
    json:encode(Message).

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

log_output(Bucket, Key, Status, Info) ->
  LogFile = log_file(Status),
  Timestamp = iso8601_timestamp(erlang:localtime()),
  Line = io_lib:format("~p,~p,~p,~p,~p~n", [Timestamp, Bucket, Key, Status, Info]),
  file:write_file(LogFile, Line, [append]).

log_file(ok) ->
    "/tmp/kafka_riak_commitlog_metrics.log";
log_file(_) ->
    "/tmp/kafka_riak_commitlog_errors.log".

iso8601_timestamp({{Year, Month, Day}, {Hour, Min, Sec}}) ->
  Format = "~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0BZ",
  FormattedIso = io_lib:format(Format, [Year, Month, Day, Hour, Min, Sec]),
  list_to_binary(FormattedIso).
