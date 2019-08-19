-module(postcommit_hook).
-export([send_to_kafka_riak_commitlog/1, send_to_commitlog/1, call_commitlog/1, do_call_commitlog/1]).

-include("src/postcommit_hook.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


%% ---------------------------------------------------------------------------
%% API
%% ---------------------------------------------------------------------------

send_to_kafka_riak_commitlog(Object) ->
    case commitlog_request(Object) of
        {ok, Request} ->
            ServerRef = {?COMMITLOG_PROCESS, commitlog_node()},
            Call = {ServerRef, Request},
            case ?MODULE:send_to_commitlog(Call) of
                ok ->
                    log(info, "call_commitlog success.", [], Call),
                    ok;
                {error, CommitlogError} ->
                    log(warn, "Unable to sync data to Commitlog: ~w.", [CommitlogError], Call),
                    {error, CommitlogError}
            end;
        {error, RiakObjectError} ->
            log(warn, "Unable to extract data from Riak object: ~w. Object: ~p.", [RiakObjectError, Object]),
            {error, RiakObjectError}
    end.

send_to_commitlog(Call) ->
    {Time, Result} = timer:tc(?MODULE, call_commitlog, [Call]),
    send_timing_to_statsd(Time, Call),
    Result.

call_commitlog(Call) ->
    try ?MODULE:do_call_commitlog(Call)
    catch _:E -> {error, E}
    end.

%% gen_server:call({kafka_riak_commitlog, 'commitlog@127.0.0.1'},
%%                 {produce, <<"store">>, <<"bucket">>, <<"key">>, <<"value">>}).
do_call_commitlog({ServerRef, Request}) ->
    gen_server:call(ServerRef, Request).


%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

-define(LOG_MSG_FMT, "[postcommit-hook] ~s ~s|~s").

log(Level, Format, Data) ->
    log(Level, Format, Data, unknown, unknown).

log(Level, Format, Data, Call) ->
    {_, {_, _, Bucket, Key, _}} = Call,
    log(Level, Format, Data, Bucket, Key).

log(info, Format, Data, Bucket, Key) ->
    error_logger:info_msg(?LOG_MSG_FMT, [io_lib:format(Format, Data), Bucket, Key]);
log(warn, Format, Data, Bucket, Key) ->
    error_logger:warning_msg(?LOG_MSG_FMT, [io_lib:format(Format, Data), Bucket, Key]).

commitlog_request(RiakObject) ->
    try
        Action = get_action(RiakObject),
        Bucket = riak_object:bucket(RiakObject),
        Key    = riak_object:key(RiakObject),
        Value  = riak_object:get_value(RiakObject),
        {ok, {produce, Action, Bucket, Key, Value}}
    catch
        _:E -> {error, E}
    end.

get_action(Object) ->
    Metadata = riak_object:get_metadata(Object),
    case dict:find(<<"X-Riak-Deleted">>, Metadata) of
        {ok, "true"} -> delete;
        _ -> store
    end.

send_timing_to_statsd(Time, Call) ->
    Message = io_lib:format("postcommit-hook-timing:~w|ms", [Time]),
    case do_send_timing_to_statsd(Message) of
        ok -> ok;
        {error, Reason} -> log(warn, "Unable to send timing to statsd: ~w.", [Reason], Call)
    end.

do_send_timing_to_statsd(Message) ->
    case gen_udp:open(0, [binary]) of
        {ok, Socket} ->
            case gen_udp:send(Socket, ?STATSD_HOST, ?STATSD_PORT, Message) of
                ok -> ok;
                {error, Reason} -> {error, {unable_to_send_to_statsd_socket, Reason}}
            end;
        {error, Reason} -> {error, {unable_to_open_statsd_socket, Reason}}
    end.

commitlog_node() ->
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

commitlog_request_test_() ->
    {foreach,
     fun() -> meck:new(riak_object, [non_strict]) end,
     fun(_) -> meck:unload(riak_object) end,
     [{"Returns ok if object is valid",
       fun() ->
               meck:expect(riak_object, get_metadata, 1, dict:new()),
               meck:expect(riak_object, bucket, 1, b),
               meck:expect(riak_object, key, 1, k),
               meck:expect(riak_object, get_value, 1, v),
               ?assertEqual({ok, {produce, store, b, k, v}}, commitlog_request(a_riak_object)),
               ?assert(meck:validate(riak_object))
       end},
      {"Returns error if object is invalid",
       fun() ->
               meck:expect(riak_object, get_metadata, 1, nil),
               ?assertMatch({error, _}, commitlog_request(a_riak_object)),
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

do_send_timing_to_statsd_test_() ->
    {foreach,
     fun() -> meck:new(gen_udp, [unstick]) end,
     fun(_) -> meck:unload(gen_udp) end,
     [{"Returns error if unable to open socket",
       fun() ->
               meck:expect(gen_udp, open, 2, {error, reason}),
               ?assertMatch({error, {unable_to_open_statsd_socket, _}},
                            do_send_timing_to_statsd("")),
               ?assert(meck:validate(gen_udp))
       end},
      {"Returns error if unable to send on socket",
       fun() ->
               meck:expect(gen_udp, open, 2, {ok, socket}),
               meck:expect(gen_udp, send, 4, {error, reason}),
               ?assertMatch({error, {unable_to_send_to_statsd_socket, _}},
                            do_send_timing_to_statsd("")),
               ?assert(meck:validate(gen_udp))
       end},
      {"Returns ok if send is successful",
       fun() ->
               meck:expect(gen_udp, open, 2, {ok, socket}),
               meck:expect(gen_udp, send, 4, ok),
               ?assertEqual(ok, do_send_timing_to_statsd("")),
               ?assert(meck:validate(gen_udp))
       end}]}.

commitlog_node_test_() ->
    [{"Uses node host name if COMMITLOG_HOSTNAME env var is not set",
      fun() ->
              ?assertEqual('commitlog@nohost', commitlog_node())
      end},
     {"Uses COMMITLOG_HOSTNAME env var if set",
      fun() ->
              os:putenv("COMMITLOG_HOSTNAME", "commitlog.test"),
              ?assertEqual('commitlog@commitlog.test', commitlog_node())
      end}].

force_separator_before_test_results_test_() ->
    Separator = "~n=======================================================~n",
    ?_assert(ok == io:format(standard_error, Separator, [])).

-endif.
