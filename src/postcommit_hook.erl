-module(postcommit_hook).
-export([send_to_kafka_riak_commitlog/1, sync_to_commitlog/4, call_commitlog/2]).

-include("src/postcommit_hook.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


%% ---------------------------------------------------------------------------
%% API
%% ---------------------------------------------------------------------------

send_to_kafka_riak_commitlog(Object) ->
    case riak_object_components(Object) of
        {ok, {Action, Bucket, Key, Value}} ->
            SyncStartTime = microtimestamp(),
            SyncResult = case ?MODULE:sync_to_commitlog(Action, Bucket, Key, Value) of
                             ok ->
                                 log(info, "sync_to_commitlog success.", [], Bucket, Key),
                                 ok;
                             {error, CommitlogError} ->
                                 log(warn, "Unable to sync data to Commitlog: ~w.", [CommitlogError], Bucket, Key),
                                 {error, CommitlogError}
                         end,
            SyncElapsedTime = microtimestamp() - SyncStartTime,
            case send_timing_to_statsd(SyncElapsedTime) of
                ok -> ok;
                {error, StatsdError} ->
                    log(warn, "Unable to send timing to statsd: ~w. Timing: ~p.", [StatsdError, SyncElapsedTime], Bucket, Key)
            end,
            SyncResult;
        {error, RiakObjectError} ->
            log(warn, "Unable to extract data from Riak object: ~w. Object: ~p.", [RiakObjectError, Object]),
            {error, RiakObjectError}
    end.

sync_to_commitlog(Action, Bucket, Key, Value) ->
    ServerRef = {?COMMITLOG_PROCESS, remote_node()},
    Request = {produce, Action, Bucket, Key, Value},
    try ?MODULE:call_commitlog(ServerRef, Request)
    catch
        _:E -> {error, E}
    end.

%% gen_server:call({kafka_riak_commitlog, 'commitlog@127.0.0.1'}, {produce, <<"store">>, <<"transactions">>, <<"key">>, <<"value for today">>}).
call_commitlog(ServerRef, Request) ->
    gen_server:call(ServerRef, Request).


%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

-define(LOG_MESSAGE_FORMAT, "[postcommit-hook] ~s ~s|~s").

log(Level, Format, Data) ->
    log(Level, Format, Data, unknown, unknown).

log(info, Format, Data, Bucket, Key) ->
    error_logger:info_msg(?LOG_MESSAGE_FORMAT, [io_lib:format(Format, Data), Bucket, Key]);
log(warn, Format, Data, Bucket, Key) ->
    error_logger:warning_msg(?LOG_MESSAGE_FORMAT, [io_lib:format(Format, Data), Bucket, Key]).

riak_object_components(Object) ->
    try
        Action = get_action(Object),
        Bucket = riak_object:bucket(Object),
        Key    = riak_object:key(Object),
        Value  = riak_object:get_value(Object),
        {ok, {Action, Bucket, Key, Value}}
    catch
        _:E -> {error, E}
    end.

get_action(Object) ->
    Metadata = riak_object:get_metadata(Object),
    case dict:find(<<"X-Riak-Deleted">>, Metadata) of
        {ok, "true"} -> delete;
        _ -> store
    end.

microtimestamp() ->
    {Mega, Sec, Micro} = os:timestamp(),
    (Mega*1000000 + Sec)*1000000 + Micro.

send_timing_to_statsd(Timing) ->
    StatsdMessage = io_lib:format("postcommit-hook-timing:~w|ms", [Timing]),

    case gen_udp:open(0, [binary]) of
        {ok, Socket} ->
            case gen_udp:send(Socket, ?STATSD_HOST, ?STATSD_PORT, StatsdMessage) of
                ok ->
                    ok;
                {error, Reason} ->
                    {error, {unable_to_send_to_statsd_socket, Reason}}
            end;
        {error, Reason} ->
            {error, {unable_to_open_statsd_socket, Reason}}
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

riak_object_components_test_() ->
    {foreach,
     fun() -> meck:new(riak_object, [non_strict]) end,
     fun(_) -> meck:unload(riak_object) end,
     [{"Returns ok if object is valid",
       fun() ->
               meck:expect(riak_object, get_metadata, 1, dict:new()),
               meck:expect(riak_object, bucket, 1, b),
               meck:expect(riak_object, key, 1, k),
               meck:expect(riak_object, get_value, 1, v),
               ?assertEqual({ok, {store, b, k, v}}, riak_object_components(a_riak_object)),
               ?assert(meck:validate(riak_object))
       end},
      {"Returns error if object is invalid",
       fun() ->
               meck:expect(riak_object, get_metadata, 1, nil),
               ?assertMatch({error, _}, riak_object_components(a_riak_object)),
               ?assert(meck:validate(riak_object))
       end}]}.

get_action_test_() ->
    {foreach,
     fun() -> meck:new(riak_object, [non_strict]) end,
     fun(_) -> meck:unload(riak_object) end,
     [{"Returns delete if X-Riak-Deleted metadata is set",
       fun() ->
               meck:expect(riak_object, get_metadata, 1,
                           dict:from_list([{<<"X-Riak-Deleted">>, "true"}])),
               ?assertEqual(delete, get_action(a_riak_object)),
               ?assert(meck:validate(riak_object))
       end},
      {"Returns store if X-Riak-Deleted metadata is not set",
       fun() ->
               meck:expect(riak_object, get_metadata, 1, dict:new()),
               ?assertEqual(store, get_action(a_riak_object)),
               ?assert(meck:validate(riak_object))
       end}]}.

send_timing_to_statsd_test_() ->
    {foreach,
     fun() -> meck:new(gen_udp, [unstick]) end,
     fun(_) -> meck:unload(gen_udp) end,
     [{"Returns error if unable to open socket",
       fun() ->
               meck:expect(gen_udp, open, 2, {error, reason}),
               ?assertMatch({error, {unable_to_open_statsd_socket, _}},
                            send_timing_to_statsd(0)),
               ?assert(meck:validate(gen_udp))
       end},
      {"Returns error if unable to send on socket",
       fun() ->
               meck:expect(gen_udp, open, 2, {ok, socket}),
               meck:expect(gen_udp, send, 4, {error, reason}),
               ?assertMatch({error, {unable_to_send_to_statsd_socket, _}},
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

force_separator_before_test_results_test_() ->
    Separator = "~n=======================================================~n",
    ?_assert(ok == io:format(standard_error, Separator, [])).

-endif.
