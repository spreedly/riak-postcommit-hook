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
              meck:new(riak_object, [non_strict]),
              meck:expect(riak_object, bucket,    fun({b, _, _}) -> b end),
              meck:expect(riak_object, key,       fun({_, k, _}) -> k end),
              meck:expect(riak_object, get_value, fun({_, _, v}) -> v end),
              meck:expect(riak_object, get_metadata, 1, dict:new())
      end,
      fun(_) ->
              meck:unload(postcommit_hook),
              meck:unload(riak_object)
      end,
      [{"Returns ok on successful send to commitlog",
        fun() ->
                Commitlog = whereis(?COMMITLOG_PROCESS),
                meck:expect(postcommit_hook, sync_to_commitlog,
                            fun(_, b, k, v) ->
                                    gen_server:call(Commitlog, {produce, store, b, k, v})
                            end),
                ?assertEqual(ok, send_to_kafka_riak_commitlog({b, k, v})),
                ?assert(meck:validate(postcommit_hook))
        end},
       {"Throws when given a bad Object",
        fun() ->
                ?assertThrow(function_clause, send_to_kafka_riak_commitlog({}))
        end},
       {"Throws when Commitlog is unavailable",
        fun() ->
                Commitlog = whereis(?COMMITLOG_PROCESS),
                meck:expect(postcommit_hook, sync_to_commitlog,
                            fun(_, _, _, _) ->
                                    gen_server:call(Commitlog, {}, 0)
                            end),
                ?assertThrow({timeout, _}, send_to_kafka_riak_commitlog({b, k, v}))
        end},
       {"Throws when statsd is unavailable",
        fun() ->
                meck:new(gen_udp, [unstick]),
                meck:expect(gen_udp, open, 2, {error, reason}),
                meck:expect(postcommit_hook, sync_to_commitlog, 4, ok),
                ?assertThrow({badmatch, _}, send_to_kafka_riak_commitlog({b, k, v})),
                ?assert(meck:validate(gen_udp)),
                meck:unload(gen_udp)
        end}]}.
