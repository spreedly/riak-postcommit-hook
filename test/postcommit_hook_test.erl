-module(postcommit_hook_test).

-import(postcommit_hook,
        [send_to_kafka_riak_commitlog/1,
         send_to_commitlog_with_retries/3,
         retry_send_to_commitlog_if_retry_enabled/4,
         retry_send_to_commitlog/3,
         send_to_commitlog_timed/1,
         send_to_commitlog/1,
         query_circuit_breaker_state/1,
         send_to_circuit_breaker/1,
         ensure_circuit_breaker_is_registered/0,
         start_circuit_breaker/0,
         stop_circuit_breaker/2,
         register_circuit_breaker/1,
         circuit_breaker_handle_call/1]).

-include_lib("eunit/include/eunit.hrl").


%% ---------------------------------------------------------------------------
%% Fake Commitlog gen_server
%% ---------------------------------------------------------------------------

-behaviour(gen_server).
-export([init/1, code_change/3, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

start_link() -> gen_server:start_link({local, commitlog}, ?MODULE, [], []).

init([]) -> {ok, []}.
code_change(_, _, _) -> ok.
handle_call(_, _, State) -> {reply, ok, State}.
handle_cast(_, _) -> ok.
handle_info(_, _) -> ok.
terminate(_, _) -> ok.


%% ---------------------------------------------------------------------------
%% Tests
%% ---------------------------------------------------------------------------

send_to_kafka_riak_commitlog_test_() ->
    {foreach,
     fun() ->
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
               meck:expect(riak_object, bucket, fun(_) -> error(invalid_riak_object) end),
               ?assertMatch({error, _}, send_to_kafka_riak_commitlog(test_riak_object)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns ok when call to Commitlog succeeds",
       fun() ->
               meck:expect(riak_object, key, 1, k2),
               meck:expect(postcommit_hook, call_commitlog, 1, ok),
               ?assertEqual(ok, send_to_kafka_riak_commitlog(test_riak_object)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns ok when statsd is unavailable",
       fun() ->
               meck:new(gen_udp, [unstick]),
               meck:expect(gen_udp, open, 2, {error, forced_by_test3}),
               meck:expect(riak_object, key, 1, k3),
               meck:expect(postcommit_hook, call_commitlog, 1, ok),
               ?assertEqual(ok, send_to_kafka_riak_commitlog(test_riak_object)),
               ?assert(meck:validate(postcommit_hook)),
               ?assert(meck:validate(gen_udp)),
               meck:unload(gen_udp)
       end},
      {"Returns error when call to Commitlog fails",
       fun() ->
               meck:expect(riak_object, key, 1, k4),
               meck:expect(postcommit_hook, call_commitlog, 1, {error, forced_failed_call}),
               ?assertMatch({error, _}, send_to_kafka_riak_commitlog(test_riak_object)),
               ?assert(meck:validate(postcommit_hook)),
               stop_circuit_breaker(whereis(circuit_breaker), failed_call_cleanup)
       end}]}.

send_to_commitlog_with_retries_test_() ->
    {foreach,
     fun() -> meck:new(postcommit_hook, [passthrough]) end,
     fun(_) -> meck:unload(postcommit_hook) end,
     [{"Returns {ok, Attempts} when call to Commitlog succeeds",
       fun() ->
               NumAttempts = 6,
               meck:expect(postcommit_hook, call_commitlog, 1, ok),
               ?assertEqual({ok, NumAttempts + 1},
                            send_to_commitlog_with_retries(a_call, NumAttempts, 0)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Retries call to Commitlog when more attempts are allowed and returns error when max attempts is reached",
       fun() ->
               Error = {error, too_many_attempts1},
               meck:expect(postcommit_hook, call_commitlog, 1, Error),
               ?assertMatch({Error, _}, send_to_commitlog_with_retries(a_call, 0, 0)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Sends failure message to Circuit Breaker when max attempts is reached",
       fun() ->
               Error = {error, too_many_attempts2},
               meck:expect(postcommit_hook, call_commitlog, 1, Error),
               ?assertMatch({retry_enabled, _}, query_circuit_breaker_state(100)),
               send_to_commitlog_with_retries(a_call, 0, 0),
               ?assertMatch({retry_disabled, _}, query_circuit_breaker_state(100)),
               ?assert(meck:validate(postcommit_hook))
       end}]}.

retry_send_to_commitlog_if_retry_enabled_test_() ->
    {foreach,
     fun() -> meck:new(postcommit_hook, [passthrough]) end,
     fun(_) -> meck:unload(postcommit_hook) end,
     [{"Calls retry_send_to_commitlog when retries are enabled",
       fun() ->
               meck:expect(postcommit_hook, query_circuit_breaker_state, 1, {retry_enabled, 0}),
               meck:expect(postcommit_hook, retry_send_to_commitlog, 3, called),
               ?assertEqual(called, retry_send_to_commitlog_if_retry_enabled({}, 0, 0, {}))
       end},
      {"Returns error when retries are disabled",
       fun() ->
               meck:expect(postcommit_hook, query_circuit_breaker_state, 1, {retry_disabled, 0}),
               Attempts = 99,
               Error = test_error,
               ?assertEqual({{error, Error}, Attempts},
                            retry_send_to_commitlog_if_retry_enabled({}, Attempts, 0, Error)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns error when Circuit Breaker query times out",
       fun() ->
               meck:expect(postcommit_hook, query_circuit_breaker_state, 1, query_timeout),
               ?assertMatch({{error, _}, _},
                            retry_send_to_commitlog_if_retry_enabled({}, 1, 0, {})),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns error when Circuit Breaker is not registered",
       fun() ->
               meck:expect(postcommit_hook, query_circuit_breaker_state, 1,
                           {error, circuit_breaker_not_registered}),
               ?assertMatch({{error, _}, _},
                            retry_send_to_commitlog_if_retry_enabled({}, 1, 0, an_error)),
               ?assert(meck:validate(postcommit_hook))
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

send_to_commitlog_test_() ->
    {foreach,
     fun() -> meck:new(postcommit_hook, [passthrough]) end,
     fun(_) -> meck:unload(postcommit_hook) end,
     [{"Returns ok when call succeeds",
       fun() ->
               meck:expect(postcommit_hook, call_commitlog, 1, ok),
               ?assertEqual(ok, send_to_commitlog(test_call)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns error when run-time error occurs",
       fun() ->
               meck:expect(postcommit_hook, call_commitlog,
                           fun(_) -> error({error, forced_by_test}) end),
               ?assertMatch({error, _}, send_to_commitlog(runtime_error_test))
       end},
      {"Returns error when Commitlog node is down",
       fun() ->
               meck:expect(postcommit_hook, call_commitlog,
                           fun(_) -> gen_server:call({commitlog, invalid_node}, {}) end),
               ?assertMatch({error, {nodedown, _}},
                            send_to_commitlog(node_down_test))
       end},
      {"Returns error when call to Commitlog times out",
       fun() ->
               start_link(),
               Commitlog = whereis(commitlog),
               meck:expect(postcommit_hook, call_commitlog,
                           fun(_) -> gen_server:call(Commitlog, {}, 0) end),
               ?assertMatch({error, timeout}, send_to_commitlog(timeout_test))
       end}]}.

%% ---------------------------------------------------------------------------

query_circuit_breaker_state_test_() ->
    {foreach,
     fun() -> meck:new(postcommit_hook, [passthrough]) end,
     fun(_) -> meck:unload(postcommit_hook) end,

     [{"Returns state when Circuit Breaker replies in time",
       fun() ->
               meck:expect(postcommit_hook, send_to_circuit_breaker, 1, ok),
               State = {test_circuit_breaker_state, test_ttl},
               self() ! {circuit_breaker, {state, State}},
               ?assertEqual(State, query_circuit_breaker_state(100)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns query_timeout when Circuit Breaker does not reply in time",
       fun() ->
               meck:expect(postcommit_hook, send_to_circuit_breaker, 1, ok),
               ?assertEqual(query_timeout, query_circuit_breaker_state(0)),
               ?assert(meck:validate(postcommit_hook))
       end},
      {"Returns error when send to Circuit Breaker fails",
       fun() ->
               Error = test_error,
               meck:expect(postcommit_hook, send_to_circuit_breaker, 1, Error),
               ?assertMatch(Error, query_circuit_breaker_state(0))
       end}]}.

send_to_circuit_breaker_test_() ->
    {foreach,
     fun() -> meck:new(postcommit_hook, [passthrough]) end,
     fun(_) -> meck:unload(postcommit_hook) end,
     [{"Returns ok when circuit_breaker is registered",
       fun() ->
               ensure_circuit_breaker_is_registered(),
               wait_until_circuit_breaker_is_registered(),
               ?assertEqual(ok, send_to_circuit_breaker(test_message1)),
               ?assert(meck:validate(postcommit_hook)),
               ensure_circuit_breaker_is_not_registered(circuit_breaker_registered)
       end},
      {"Returns error when Circuit Breaker is not registered",
       fun() ->
               meck:expect(postcommit_hook, ensure_circuit_breaker_is_registered, 0,
                           not_actually_registered),
               ?assertMatch({error, circuit_breaker_not_registered},
                            send_to_circuit_breaker(test_message2)),
               ?assert(meck:validate(postcommit_hook))
       end}]}.

ensure_circuit_breaker_is_registered_test_() ->
    {foreach,
     fun() -> ok end,
     fun(_) -> ensure_circuit_breaker_is_not_registered(test_cleanup) end,
     [{"Returns circuit_breaker pid",
       fun() ->
               Pid = ensure_circuit_breaker_is_registered(),
               wait_until_circuit_breaker_is_registered(),
               ?assertEqual(Pid, whereis(circuit_breaker))
       end},
      {"Returns circuit_breaker pid when startup race condition occurs",
       fun() ->
               meck:new(postcommit_hook, [passthrough]),
               meck:expect(postcommit_hook, register_circuit_breaker,
                           fun(Pid) ->
                                   register(circuit_breaker, Pid),
                                   {error, force_loop}
                           end),
               meck:expect(postcommit_hook, stop_circuit_breaker, 2, do_not_stop),
               Pid = ensure_circuit_breaker_is_registered(),
               ?assertEqual(Pid, whereis(circuit_breaker)),
               meck:unload(postcommit_hook)
       end},
      {"Stops spurious circuit_breaker when startup race conditon occurs",
       fun() ->
               meck:new(postcommit_hook, [passthrough]),
               meck:expect(postcommit_hook, register_circuit_breaker, 1, forced_by_test),
               meck:expect(postcommit_hook, stop_circuit_breaker, fun(_, Error) -> error(Error) end),
               ?assertError(forced_by_test, ensure_circuit_breaker_is_registered()),
               meck:unload(postcommit_hook)
       end}]}.

register_circuit_breaker_test_() ->
    {foreach,
     fun() -> ensure_circuit_breaker_is_not_registered(test_setup) end,
     fun(_) -> ensure_circuit_breaker_is_not_registered(test_cleanup) end,
     [{"Returns ok when not already registered",
       fun() ->
               Pid = start_circuit_breaker(),
               ?assertEqual(ok, register_circuit_breaker(Pid)),
               ?assertEqual(Pid, wait_until_circuit_breaker_is_registered())
       end},
      {"Returns error when already registered",
       fun() ->
               Pid1 = start_circuit_breaker(),
               Pid2 = start_circuit_breaker(),
               register_circuit_breaker(Pid1),
               ?assertEqual(Pid1, wait_until_circuit_breaker_is_registered()),
               ?assertMatch({error, _}, register_circuit_breaker(Pid2)),
               stop_circuit_breaker(Pid2, already_registered)
       end},
      {"Returns error when pid is invalid",
       fun() ->
               InvalidPid = list_to_pid("<0.32767.8>"),
               ?assertMatch({error, _}, register_circuit_breaker(InvalidPid))
       end},
      {"Registers circuit_breaker with Pid when not already registered (race condition)",
       fun() ->
               Pid1 = start_circuit_breaker(),
               Pid2 = start_circuit_breaker(),
               spawn(fun() -> register_circuit_breaker(Pid1) end),
               spawn(fun() -> register_circuit_breaker(Pid2) end),
               WonTheRace = wait_until_circuit_breaker_is_registered(),
               LostTheRace = case WonTheRace of Pid1 -> Pid2; Pid2 -> Pid1 end,
               ?assertNotEqual(undefined, WonTheRace),
               ?assertNotEqual(undefined, LostTheRace),
               ?assert(lists:member(WonTheRace, [Pid1, Pid2])),
               ?assertEqual(WonTheRace, ensure_circuit_breaker_is_registered()),
               stop_circuit_breaker(LostTheRace, lost_the_race)
       end}]}.

circuit_breaker_handle_call_test_() ->
    {foreach,
     fun() -> ok end,
     fun(_) -> ok end,
     [
      {"Retries are enabled and TTL is zero immediately aftering starting",
       fun() ->
               Pid = start_circuit_breaker(),
               ?assertMatch({circuit_breaker, {state, {retry_enabled, 0}}}, get_cb_state(Pid)),
               stop_circuit_breaker(Pid, query_test_cleanup)
       end},
      {"Disables retries when sent a failure message",
       fun() ->
               Pid = start_circuit_breaker(),
               ?assertMatch({_, {_, {retry_enabled, _}}}, get_cb_state(Pid)),
               Pid ! {self(), {result, failure}},
               ?assertMatch({_, {_, {retry_disabled, _}}}, get_cb_state(Pid)),
               stop_circuit_breaker(Pid, failure_test_cleanup)
       end},
      {"Disables retries when sent a failure message",
       fun() ->
               Pid = start_circuit_breaker(),
               ?assertMatch({circuit_breaker, {state, {retry_enabled, _}}}, get_cb_state(Pid)),
               Pid ! {self(), {result, failure}},
               ?assertMatch({circuit_breaker, {state, {retry_disabled, _}}}, get_cb_state(Pid)),
               stop_circuit_breaker(Pid, disabled_retries_test_cleanup)
       end},
      {"Extends TTL if sent a failure messages when retries are disabled",
       fun() ->
               Pid = start_circuit_breaker(),
               Pid ! {self(), {result, failure}},
               timer:sleep(100),
               {_, {_, {_, TTL}}} = get_cb_state(Pid),
               Pid ! {self(), {result, failure}},
               {_, {_, {_, ResetTTL}}} = get_cb_state(Pid),
               ?assert(TTL < ResetTTL),
               stop_circuit_breaker(Pid, extended_ttl_test_cleanup)
       end},
      {"Sets a timer if sent a timeout message when retries are disabled",
       fun() ->
               process_flag(trap_exit, true),
               Pid = spawn_link(postcommit_hook, circuit_breaker_handle_call, [{unknown, 100}]),
               Pid ! {self(), {result, failure}},
               Pid ! {self(), timeout},
               ?assertMatch({'EXIT', _, _}, receive Msg -> Msg end)
       end},
      {"Exits if sent a timeout message when retries are enabled",
       fun() ->
               process_flag(trap_exit, true),
               Pid = spawn_link(postcommit_hook, circuit_breaker_handle_call, [{unknown, 1000}]),
               Pid ! {self(), timeout},
               ?assertMatch({'EXIT', _, _}, receive Msg -> Msg end)
       end},
      {"Exits when sent a stop message",
       fun() ->
               process_flag(trap_exit, true),
               Pid = spawn_link(postcommit_hook, circuit_breaker_handle_call, [{unknown, 1000}]),
               ?assertNotEqual(undefined, process_info(Pid)),
               Pid ! {self(), {stop, exit_test}},
               ?assertMatch({'EXIT', _, _}, receive Msg -> Msg end)
       end}]}.


%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

get_cb_state(Pid) ->
    Pid ! {self(), {query, state}},
    receive State -> State end.

ensure_circuit_breaker_is_not_registered(Reason) ->
    case whereis(circuit_breaker) of
        undefined -> ok;
        Pid -> stop_circuit_breaker(Pid, Reason),
               ensure_circuit_breaker_is_not_registered(Reason)
    end.

wait_until_circuit_breaker_is_registered() ->
    case whereis(circuit_breaker) of
        undefined -> wait_until_circuit_breaker_is_registered();
        Pid -> Pid
    end.
