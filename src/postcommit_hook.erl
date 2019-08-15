-module(postcommit_hook).
-export([send_to_kafka_riak_commitlog/1, sync_to_commitlog/4, riak_object_components/1]).

-include("src/postcommit_hook.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


%% ---------------------------------------------------------------------------
%% API
%% ---------------------------------------------------------------------------

send_to_kafka_riak_commitlog(Object) ->
    {ok, {Action, Bucket, Key, Value}} = ?MODULE:riak_object_components(Object),

    SyncStartMicrotime = microtimestamp(),
    try
        ok = ?MODULE:sync_to_commitlog(Action, Bucket, Key, Value),
        error_logger:info_msg("[postcommit-hook] sync_to_commitlog success. Bucket: ~p. Key: ~p.", [Bucket, Key]),
        ok
    catch
        _:SyncException ->
            error_logger:warning_msg("[postcommit-hook] Unable to sync data to Commitlog: ~p. Bucket: ~p. Key: ~p.", [SyncException, Bucket, Key]),
            throw(SyncException)
    after
        SyncMicrotime = microtimestamp() - SyncStartMicrotime,
        try
            ok = send_timing_to_statsd(SyncMicrotime)
        catch
            _:TimingException ->
                error_logger:warning_msg("[postcommit-hook] Unable to send timing to statsd: ~p. Timing: ~p. Bucket: ~p. Key: ~p.",
                                         [TimingException, SyncMicrotime, Bucket, Key]),
                throw(TimingException)
        end
    end,
    ok.


%% gen_server:call({kafka_riak_commitlog, 'commitlog@127.0.0.1'}, {produce, <<"store">>, <<"transactions">>, <<"key">>, <<"value for today">>}).
sync_to_commitlog(Action, Bucket, Key, Value) ->
    ServerRef = {?COMMITLOG_PROCESS, remote_node()},
    Request = {produce, Action, Bucket, Key, Value},
    ok = gen_server:call(ServerRef, Request).

riak_object_components(Object) ->
    try
        Action = get_action(Object),
        Bucket = riak_object:bucket(Object),
        Key    = riak_object:key(Object),
        Value  = riak_object:get_value(Object),
        {ok, {Action, Bucket, Key, Value}}
    catch
        Class:Error ->
            error_logger:warning_msg("[postcommit-hook] Unable to extract data from Riak object: ~p. Object: ~p.",
                                     [{Class, Error}, Object]),
            {error, {Class, Error}}
    end.

%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

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
               meck:expect(riak_object, get_metadata, fun(_) -> error(e) end),
               ?assertMatch({error, _}, riak_object_components(a_riak_object))
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
