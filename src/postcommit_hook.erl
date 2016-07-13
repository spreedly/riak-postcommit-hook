-module(postcommit_hook).
-export([send_to_kafka_riak_commitlog/1, sync_to_commitlog/4]).

-define(COMMITLOG_PROCESS, kafka_riak_commitlog).
-define(STATSD_HOST, "127.0.0.1").
-define(STATSD_PORT, 8125).

%% ---------------------------------------------------------------------------
%% API
%% ---------------------------------------------------------------------------

send_to_kafka_riak_commitlog(Object) ->
    {Action, Bucket, Key, Value} = try
        {get_action(Object), get_bucket(Object), get_key(Object), get_value(Object)}
    catch
        _:ExtractException ->
            error_logger:warning_msg("[commitlog] Unable to extract data from Riak object: ~p. Object: ~p.", [ExtractException, Object]),
            throw(ExtractException)
    end,

    TimingResult = try
        {Timing, ok} = timer:tc(?MODULE, sync_to_commitlog, [Action, Bucket, Key, Value]),
        Timing
    catch
        _:SyncException ->
            error_logger:warning_msg("[commitlog] Unable to sync data to commitlog: ~p. Bucket: ~p. Key: ~p.", [SyncException, Bucket, Key]),
            throw(SyncException)
    end,

    try
        ok = send_timing_to_statsd(TimingResult)
    catch
        _:TimingException ->
            error_logger:warning_msg("[commitlog] Unable to send timing to statsd: ~p. Timing: ~p. Bucket: ~p. Key: ~p.", [TimingException, TimingResult, Bucket, Key]),
            throw(TimingException)
    end.

%% gen_server:call({kafka_riak_commitlog, 'commitlog@127.0.0.1'}, {produce, <<"store">>, <<"transactions">>, <<"key">>, <<"value for today">>}).
sync_to_commitlog(Action, Bucket, Key, Value) ->
    Remote = {?COMMITLOG_PROCESS, remote_node()},
    Body = {produce, Action, Bucket, Key, Value},
    ok = gen_server:call(Remote, Body).

%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

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

send_timing_to_statsd(Timing) ->
    StatsdMessage = io_lib:format("postcommit-hook-timing:~w|ms", [Timing]),

    ok = case gen_udp:open(0, [binary]) of
            {ok, Socket} ->
                 case gen_udp:send(Socket, ?STATSD_HOST, ?STATSD_PORT, StatsdMessage) of
                     ok ->
                         ok;
                     Error ->
                         {unable_to_send_to_statsd_socket, Error}
                 end;
            Error ->
                {unable_to_open_statsd_socket, Error}
         end.

remote_node() ->
    [_Name, IP] = string:tokens(atom_to_list(node()), "@"),
    Remote = "commitlog" ++ "@" ++ IP,
    list_to_atom(Remote).

