-module(postcommit_hook_test).

-behaviour(gen_server).
-export([init/1, code_change/3, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-import(postcommit_hook, [send_to_kafka_riak_commitlog/1, sync_to_commitlog/4]).
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
              meck:expect(postcommit_hook, riak_object_components, 1, {ok, {store, b, k, v}}),
              meck:new(gen_udp, [unstick]),
              meck:expect(gen_udp, open, 2, {ok, socket}),
              meck:expect(gen_udp, send, 4, ok)
      end,
      fun(_) ->
              meck:unload(postcommit_hook),
              meck:unload(gen_udp)
      end,
      [{"Returns ok on successful send to commitlog",
        fun() ->
                Commitlog = whereis(?COMMITLOG_PROCESS),
                meck:expect(postcommit_hook, sync_to_commitlog,
                            fun(store, b, k, v) -> gen_server:call(Commitlog, a_request) end),
                ?assertEqual(ok, send_to_kafka_riak_commitlog(a_riak_object)),
                ?assert(meck:validate(postcommit_hook))
        end},
       {"Throws when Commitlog is unavailable",
        fun() ->
                Commitlog = whereis(?COMMITLOG_PROCESS),
                meck:expect(postcommit_hook, sync_to_commitlog,
                            fun(store, b, k, v) -> gen_server:call(Commitlog, a_request, 0) end),
                ?assertThrow({timeout, _}, send_to_kafka_riak_commitlog(a_riak_object))
        end},
       {"Throws when statsd is unavailable",
        fun() ->
                meck:expect(gen_udp, open, 2, {error, reason}),
                meck:expect(postcommit_hook, sync_to_commitlog, 4, ok),
                ?assertThrow({badmatch, _}, send_to_kafka_riak_commitlog({b, k, v})),
                ?assert(meck:validate(gen_udp))
        end}]}.
