-module(postcommit_hook).
-export([send_to_kafka_riak_commitlog/1, sync_to_commitlog/4]).

-include("src/postcommit_hook.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


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
        error_logger:info_msg("[commitlog] sync_to_commitlog success. Bucket: ~p. Key: ~p.", [Bucket, Key]),
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
    ServerRef = {?COMMITLOG_PROCESS, remote_node()},
    Request = {produce, Action, Bucket, Key, Value},
    ok = gen_server:call(ServerRef, Request).

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

    case gen_udp:open(0, [binary]) of
        {ok, Socket} ->
            case gen_udp:send(Socket, ?STATSD_HOST, ?STATSD_PORT, StatsdMessage) of
                ok ->
                    ok;
                {error, Reason} ->
                    {error, unable_to_send_to_statsd_socket, Reason}
            end;
        {error, Reason} ->
            {error, unable_to_open_statsd_socket, Reason}
    end.

remote_node() ->
    Hostname = case os:getenv("COMMITLOG_HOSTNAME") of
                   false -> [_Name, Host] = string:tokens(atom_to_list(node()), "@"),
                            Host;
                   EnvHost -> EnvHost
               end,
    Remote = "commitlog" ++ "@" ++ Hostname,
    list_to_atom(Remote).

%% ---------------------------------------------------------------------------
%% Tests
%% ---------------------------------------------------------------------------

-ifdef(TEST).

get_test_() ->
    {foreach,
     fun() -> meck:new(riak_object, [non_strict]) end,
     fun(_) -> meck:unload(riak_object) end,
     [{"get_action: delete",
       fun() ->
               meck:expect(riak_object, get_metadata, 1,
                           dict:from_list([{<<"X-Riak-Deleted">>, "true"}])),
               ?assertEqual(delete, get_action(a_riak_object)),
               ?assert(meck:validate(riak_object))
       end},
      {"get_action: store",
       fun() ->
               meck:expect(riak_object, get_metadata, 1, dict:new()),
               ?assertEqual(store, get_action(a_riak_object)),
               ?assert(meck:validate(riak_object))
       end},
      {"get_bucket",
       fun() ->
               meck:expect(riak_object, bucket, 1, "bucket"),
               ?assertEqual("bucket", get_bucket(a_riak_object)),
               ?assert(meck:validate(riak_object))
       end},
      {"get_key",
       fun() ->
               meck:expect(riak_object, key, 1, "key"),
               ?assertEqual("key", get_key(a_riak_object)),
               ?assert(meck:validate(riak_object))
       end},
      {"get_value",
       fun() ->
               meck:expect(riak_object, get_value, 1, "value"),
               ?assertEqual("value", get_value(a_riak_object)),
               ?assert(meck:validate(riak_object))
       end}]}.

send_timing_to_statsd_test_() ->
    {foreach,
     fun() -> meck:new(gen_udp, [unstick]) end,
     fun(_) -> meck:unload(gen_udp) end,
     [{"Returns error if unable to open socket",
       fun() ->
               meck:expect(gen_udp, open, 2, {error, reason}),
               ?assertEqual({error, unable_to_open_statsd_socket, reason},
                            send_timing_to_statsd(0)),
               ?assert(meck:validate(gen_udp))
       end},
      {"Returns error if unable to send on socket",
       fun() ->
               meck:expect(gen_udp, open, 2, {ok, socket}),
               meck:expect(gen_udp, send, 4, {error, reason}),
               ?assertEqual({error, unable_to_send_to_statsd_socket, reason},
                            send_timing_to_statsd(0)),
               ?assert(meck:validate(gen_udp))
       end},
      {"Returns ok if send is successful",
       fun() ->
               meck:expect(gen_udp, open, 2, {ok, socket}),
               meck:expect(gen_udp, send, 4, ok),
               ?assertEqual(ok, send_timing_to_statsd(0)),
               ?assert(meck:validate(gen_udp))
       end}]}.

remote_node_test_() ->
    [{"Uses node host name if COMMITLOG_HOSTNAME env var is not set",
      fun() ->
              ?assertEqual('commitlog@nohost', remote_node())
      end},
     {"Uses COMMITLOG_HOSTNAME env var if set",
      fun() ->
              os:putenv("COMMITLOG_HOSTNAME", "commitlog.test"),
              ?assertEqual('commitlog@commitlog.test', remote_node())
      end}].

-endif.
