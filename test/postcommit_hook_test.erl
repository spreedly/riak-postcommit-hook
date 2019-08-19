-module(postcommit_hook_test).

-behaviour(gen_server).
-export([init/1, code_change/3, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-import(postcommit_hook, [send_to_kafka_riak_commitlog/1, sync_to_commitlog/1]).
-include("src/postcommit_hook.hrl").
-include_lib("eunit/include/eunit.hrl").

start_link() -> gen_server:start_link({local, ?COMMITLOG_PROCESS}, ?MODULE, [], []).
init([]) -> {ok, []}.
code_change(_, _, _) -> ok.
handle_call(_, _, State) -> {reply, ok, State}.
handle_cast(_, _) -> ok.
handle_info(_, _) -> ok.
terminate(_, _) -> ok.

send_to_kafka_riak_commitlog_test_() ->
    {foreach,
     fun() ->
             start_link(),
             meck:new(postcommit_hook, [passthrough, non_strict]),
             meck:new(riak_object, [non_strict]),
             meck:expect(riak_object, get_metadata, 1, dict:new()),
             meck:expect(riak_object, bucket, 1, b),
             meck:expect(riak_object, key, 1, k),
             meck:expect(riak_object, get_value, 1, v)
     end,
     fun(_) ->
             meck:unload(postcommit_hook),
             meck:unload(riak_object)
     end,
     [{"Returns error when Riak object is invalid",
       fun() ->
               meck:expect(riak_object, bucket, 1, fun(_) -> error(forced_by_test1) end),
               ?assertMatch({error, _}, send_to_kafka_riak_commitlog(test_riak_object1)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns ok when call to Commitlog succeeds",
       fun() ->
               meck:expect(riak_object, key, 1, k2),
               meck:expect(postcommit_hook, call_commitlog, 2, ok),
               ?assertEqual(ok, send_to_kafka_riak_commitlog(test_riak_object2)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns ok when statsd is unavailable",
       fun() ->
               meck:new(gen_udp, [unstick]),
               meck:expect(gen_udp, open, 2, {error, forced_by_test3}),
               meck:expect(riak_object, key, 1, k3),
               meck:expect(postcommit_hook, call_commitlog, 2, ok),
               ?assertEqual(ok, send_to_kafka_riak_commitlog(test_riak_object3)),
               ?assert(meck:validate(postcommit_hook)),
               ?assert(meck:validate(gen_udp)),
               meck:unload(gen_udp)
       end},
      {"Returns error when call to Commitlog fails",
       fun() ->
               meck:expect(riak_object, key, 1, k4),
               meck:expect(postcommit_hook, call_commitlog, 2, {error, forced_by_test4}),
               ?assertMatch({error, _}, send_to_kafka_riak_commitlog(test_riak_object4)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns error when Commitlog node is down",
       fun() ->
               meck:expect(riak_object, key, 1, k5),
               meck:expect(postcommit_hook, call_commitlog,
                           fun(_ServerRef, Req) ->
                                   gen_server:call({invalid_name, 'invalid_node'}, Req)
                           end),
               ?assertMatch({error, {{nodedown, _}, _}}, send_to_kafka_riak_commitlog(test_riak_object5))
       end},
      {"Returns error when call to Commitlog times out",
       fun() ->
               Commitlog = whereis(?COMMITLOG_PROCESS),
               meck:expect(riak_object, key, 1, k6),
               meck:expect(postcommit_hook, call_commitlog,
                           fun(_ServerRef, Req) ->
                                   gen_server:call(Commitlog, Req, 0)
                           end),
               ?assertMatch({error, {timeout, _}}, send_to_kafka_riak_commitlog(test_riak_object6))
       end}
     ]}.

sync_to_commitlog_test_() ->
    {foreach,
     fun() -> meck:new(postcommit_hook, [passthrough]) end,
     fun(_) -> meck:unload(postcommit_hook) end,
     [{"Passes expected ServerRef to call_commitlog",
       fun() ->
               meck:expect(postcommit_hook, call_commitlog, fun(ServerRef, _) -> ServerRef end),
               ?assertEqual({?COMMITLOG_PROCESS, 'commitlog@nohost'}, sync_to_commitlog(a_request)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns ok when call_commitlog succeeds",
       fun() ->
               meck:expect(postcommit_hook, call_commitlog, 2, ok),
               ?assertEqual(ok, sync_to_commitlog(a_request)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns error when call_commitlog fails",
       fun() ->
               meck:expect(postcommit_hook, call_commitlog, fun(_, _) -> exit(forced_by_test) end),
               ?assertMatch({error, _}, sync_to_commitlog(a_request))
       end}
     ]}.
