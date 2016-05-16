-module(kafka_riak_commitlog).
-export([postcommit_hook/1, call_to_remote_node/4, cast_to_remote_node/4]).

-define(OTP_PROCESS, kafka_riak_commitlog).

%% ---------------------------------------------------------------------------
%% API
%% ---------------------------------------------------------------------------

postcommit_hook(Object) ->
    Action = get_action(Object),
    Bucket = get_bucket(Object),
    Key = get_key(Object),
    Value = get_value(Object),
    ok = call_to_remote_node(Action, Bucket, Key, Value).

call_to_remote_node(Action, Bucket, Key, Value) ->
    %% gen_server:call({kafka_riak_commitlog, 'commitlog_1@127.0.0.1'}, {produce, <<"store">>, <<"transactions">>, <<"key">>, <<"value for today">>}).
    Remote = {?OTP_PROCESS, remote_node()},
    Body = {produce, Action, Bucket, Key, Value},
    gen_server:call(Remote, Body).

cast_to_remote_node(Action, Bucket, Key, Value) ->
    %% gen_server:cast({kafka_riak_commitlog, 'commitlog_1@127.0.0.1'}, {produce, <<"store">>, <<"transactions">>, <<"key">>, <<"value for today">>}).
    Remote = {?OTP_PROCESS, remote_node()},
    Body = {produce, Action, Bucket, Key, Value},
    gen_server:cast(Remote, Body).


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

remote_node() ->
    [Name, IP] = string:tokens(atom_to_list(node()), "@"),
    [_, _, N] = string:tokens(Name, "_"),
    Remote = "commitlog_" ++ N ++ "@" ++ IP,
    list_to_atom(Remote).
