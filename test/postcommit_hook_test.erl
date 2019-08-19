-module(postcommit_hook_test).

-behaviour(gen_server).
-export([init/1, code_change/3, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-import(postcommit_hook,
        [send_to_kafka_riak_commitlog/1,
         send_to_commitlog_with_retries/3,
         retry_send_to_commitlog/3,
         send_to_commitlog/1,
         call_commitlog/1]).

-include_lib("eunit/include/eunit.hrl").

start_link() -> gen_server:start_link({local, commitlog}, ?MODULE, [], []).
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
             meck:expect(postcommit_hook, retry_send_to_commitlog,
                         % Disable retry delay
                         fun(Call, Attempts, _Delay) ->
                                 send_to_commitlog_with_retries(Call, Attempts, 0)
                         end),
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
               meck:expect(riak_object, bucket, fun(_) -> error(forced_by_test1) end),
               ?assertMatch({error, _}, send_to_kafka_riak_commitlog(test_riak_object1)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns ok when call to Commitlog succeeds",
       fun() ->
               meck:expect(riak_object, key, 1, k2),
               meck:expect(postcommit_hook, do_call_commitlog, 1, ok),
               ?assertEqual(ok, send_to_kafka_riak_commitlog(test_riak_object2)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns ok when statsd is unavailable",
       fun() ->
               meck:new(gen_udp, [unstick]),
               meck:expect(gen_udp, open, 2, {error, forced_by_test3}),
               meck:expect(riak_object, key, 1, k3),
               meck:expect(postcommit_hook, do_call_commitlog, 1, ok),
               ?assertEqual(ok, send_to_kafka_riak_commitlog(test_riak_object3)),
               ?assert(meck:validate(postcommit_hook)),
               ?assert(meck:validate(gen_udp)),
               meck:unload(gen_udp)
       end},
      {"Returns error when call to Commitlog fails",
       fun() ->
               meck:expect(riak_object, key, 1, k4),
               meck:expect(postcommit_hook, do_call_commitlog, 1, {error, forced_by_test4}),
               ?assertMatch({error, _}, send_to_kafka_riak_commitlog(test_riak_object4)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns error when Commitlog node is down",
       fun() ->
               meck:expect(riak_object, key, 1, k5),
               meck:expect(postcommit_hook, do_call_commitlog,
                           fun(_) -> gen_server:call({commitlog, invalid_node}, {}) end),
               ?assertMatch({error, {nodedown, _}}, send_to_kafka_riak_commitlog(test_riak_object5))
       end},
      {"Returns error when call to Commitlog times out",
       fun() ->
               Commitlog = whereis(commitlog),
               meck:expect(riak_object, key, 1, k6),
               meck:expect(postcommit_hook, do_call_commitlog,
                           fun(_) -> gen_server:call(Commitlog, {}, 0) end),
               ?assertMatch({error, timeout}, send_to_kafka_riak_commitlog(test_riak_object6))
       end}]}.

retry_send_to_commitlog_test_() ->
    {foreach,
     fun() -> meck:new(postcommit_hook, [passthrough]) end,
     fun(_) -> meck:unload(postcommit_hook) end,
     [{"Calls send_to_commitlog_with_retries with Delay increased",
       fun() ->
               meck:expect(postcommit_hook, send_to_commitlog_with_retries,
                           fun(_, _, Delay) -> Delay end),
               Delay = 100,
               ?assert(Delay < retry_send_to_commitlog({}, 0, Delay)),
               ?assert(meck:validate(postcommit_hook))
       end}]}.

call_commitlog_test_() ->
    {foreach,
     fun() -> meck:new(postcommit_hook, [passthrough]) end,
     fun(_) -> meck:unload(postcommit_hook) end,
     [{"Returns ok when call succeeds",
       fun() ->
               meck:expect(postcommit_hook, do_call_commitlog, 1, ok),
               ?assertEqual(ok, call_commitlog(a_request)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns error when call fails",
       fun() ->
               meck:expect(postcommit_hook, do_call_commitlog, fun(_) -> exit(forced_by_test) end),
               ?assertMatch({error, _}, call_commitlog(a_request))
       end}]}.
