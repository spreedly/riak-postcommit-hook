-module(postcommit_hook).
-export([send_to_kafka_riak_commitlog/1, sync_to_commitlog/4]).

-define(COMMITLOG_PROCESS, kafka_riak_commitlog).
-define(STATSD_HOST, "127.0.0.1").
-define(STATSD_PORT, 8125).

%% ---------------------------------------------------------------------------
%% API
%% ---------------------------------------------------------------------------

send_to_kafka_riak_commitlog(Object) ->
    try
        Action = get_action(Object),
        Bucket = get_bucket(Object),
        Key = get_key(Object),
        Value = get_value(Object),
        {Timing, ok} = timer:tc(?MODULE, sync_to_commitlog, [Action, Bucket, Key, Value]),
        send_timing_to_statsd(Timing)
    catch
        _Type:Exception ->
            error_logger:error_msg("Error running postcommit hook: ~p. Object: ~p", [Exception, Object]),
            throw(Exception)
    end.

sync_to_commitlog(Action, Bucket, Key, Value) ->
    %% gen_server:call({kafka_riak_commitlog, 'commitlog_1@127.0.0.1'}, {produce, <<"store">>, <<"transactions">>, <<"key">>, <<"value for today">>}).
    Remote = {?COMMITLOG_PROCESS, remote_node()},
    Body = {produce, Action, Bucket, Key, Value},
    gen_server:call(Remote, Body).

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
    {ok, Socket} = gen_udp:open(0, [binary]),
    StatsdMessage = io_lib:format("postcommit-hook-timing:~w|ms", [Timing]),
    ok = gen_udp:send(Socket, ?STATSD_HOST, ?STATSD_PORT, StatsdMessage).

remote_node() ->
    [_Name, IP] = string:tokens(atom_to_list(node()), "@"),
    Remote = "commitlog" ++ "@" ++ IP,
    list_to_atom(Remote).

