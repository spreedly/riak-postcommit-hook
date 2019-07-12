-module(postcommit_hook).
-export([attempt_send_to_kafka_riak_commitlog/3, send_to_kafka_riak_commitlog/1, sync_to_commitlog/4]).

-define(COMMITLOG_PROCESS, kafka_riak_commitlog).
-define(STATSD_HOST, "127.0.0.1").
-define(STATSD_PORT, 8125).

-define(INITIAL_RETRY_DELAY_MS, 100).
-define(MAX_RETRIES, 6).
-define(RETRY_DELAY_JITTER, 0.15).
-define(RETRY_DELAY_MULTIPLIER, 2.0).

%% ---------------------------------------------------------------------------
%% API
%% ---------------------------------------------------------------------------

send_to_kafka_riak_commitlog(Object) ->
    send_to_kafka_riak_commitlog_with_retries(Object, timestamp(), 0, ?INITIAL_RETRY_DELAY_MS),
    ok.

attempt_send_to_kafka_riak_commitlog(Object, StartTime, RetryCount) ->
    {Action, Bucket, Key, Value} = try
        {get_action(Object), get_bucket(Object), get_key(Object), get_value(Object)}
    catch
        _:ExtractException ->
            error_logger:warning_msg("[postcommit-hook] Unable to extract data from Riak object: ~s. Object: ~s.",
                                     [ExtractException, Object]),
            throw({e_extract, ExtractException})
    end,

    TimingResult = try
        {Timing, ok} = timer:tc(?MODULE, sync_to_commitlog, [Action, Bucket, Key, Value]),
        error_logger:info_msg("   [postcommit-hook] sync_to_commitlog success. Time: ~5.. B ms. Retries: ~p. ~s|~s",
                              [timestamp()-StartTime, RetryCount, Bucket, Key]),
        Timing
    catch
        _:SyncException ->
            {SyncErrorReason, _} = SyncException,
            error_logger:warning_msg("[postcommit-hook] Unable to sync data to Commitlog: ~p. Time: ~5.. B ms. Retries: ~p. ~s|~s",
                                     [SyncErrorReason, timestamp()-StartTime, RetryCount, Bucket, Key]),
            throw({e_sync, SyncException})
    end,

    try
        ok = send_timing_to_statsd(TimingResult)
    catch
        _:TimingException ->
            error_logger:warning_msg("[postcommit-hook] Unable to send timing to statsd: ~p. Timing: ~p. ~s|~s",
                                     [TimingException, TimingResult, Bucket, Key]),
            throw({e_timing, TimingException})
    end.

%% gen_server:call({kafka_riak_commitlog, 'commitlog@127.0.0.1'}, {produce, <<"store">>, <<"transactions">>, <<"key">>, <<"value for today">>}).
sync_to_commitlog(Action, Bucket, Key, Value) ->
    Remote = {?COMMITLOG_PROCESS, remote_node()},
    Body = {produce, Action, Bucket, Key, Value},
    ok = gen_server:call(Remote, Body).

%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

%% https://gist.github.com/DimitryDushkin/5532071
timestamp() ->
  {Mega, Sec, Micro} = os:timestamp(),
  (Mega*1000000 + Sec)*1000 + round(Micro/1000).

send_to_kafka_riak_commitlog_with_retries(Object, StartTime, RetryCount, RetryDelay) ->
    try
        attempt_send_to_kafka_riak_commitlog(Object, StartTime, RetryCount),
        send_to_retry_manager({self(), result, success})
    catch
        {e_sync, SyncException} when RetryCount < ?MAX_RETRIES ->
            timer:sleep(RetryDelay),
            seed_if(RetryCount == 0),
            case query_retry_state() of
                retry_enabled ->
                    NextRetryDelay = jitter(RetryDelay * ?RETRY_DELAY_MULTIPLIER, ?RETRY_DELAY_JITTER),
                    send_to_kafka_riak_commitlog_with_retries(Object, StartTime, RetryCount + 1, NextRetryDelay);
                _RetryDisabledOrQueryTimeout ->
                    log_sync_failure(SyncException, Object, StartTime, RetryCount)
            end;
        {e_sync, SyncException} ->
            log_sync_failure(SyncException, Object, StartTime, RetryCount),
            send_to_retry_manager({self(), result, failure}),
            SyncException;
        E ->
            E
    end.

log_sync_failure(SyncException, Object, StartTime,RetryCount) ->
    {SyncErrorReason, _} = SyncException,
    {Bucket, Key} = {get_bucket(Object), get_key(Object)},
    error_logger:warning_msg("[postcommit-hook] FAILED to sync data to Commitlog: ~p. Time: ~5.. B ms. Retries: ~p. ~s|~s",
                             [SyncErrorReason, timestamp()-StartTime, RetryCount, Bucket, Key]).

query_retry_state() ->
    send_to_retry_manager({self(), query, retry_state}),
    receive
        {retry_state, RetryState} ->
            RetryState
    after
        1000 ->
            query_timeout
    end.

send_to_retry_manager(Message) ->
    ensure_retry_manager_is_registered(),
    retry_manager ! Message.

ensure_retry_manager_is_registered() ->
    case whereis(retry_manager) of
        undefined ->
            Pid = spawn(fun() -> start_retry_manager() end),
            try register(retry_manager, Pid)
            catch _:badarg -> ensure_retry_manager_is_registered()
            end,
            Pid;
        Pid ->
            Pid
    end.

start_retry_manager() ->
    error_logger:info_msg("   [pch-retry-manager] Retry manager started."),
    retry_manager_loop(retry_enabled).

retry_manager_loop(RetryState) ->
    receive
        {From, query, retry_state} ->
            From ! {retry_state, RetryState},
            retry_manager_loop(RetryState);
        {_, result, Result} ->
            NewRetryState = case {RetryState, Result} of
                                {retry_enabled, failure} ->
                                    error_logger:warning_msg("[pch-retry-manager] Retry disabled."),
                                    retry_disabled;
                                {retry_disabled, success} ->
                                    error_logger:info_msg("  [pch-retry-manager] Retry enabled."),
                                    retry_enabled;
                                _Else ->
                                    RetryState
                            end,
            retry_manager_loop(NewRetryState)
    end.

%% Seeding is necessary to avoid a race condition where multiple postcommit-
%% hook processes automatically seed the RNG in the same manner and all use the
%% same seed, which defeats the purpose of adding jitter.
seed_if(true) ->
    {_, Seconds, MicroSecs} = now(),
    random:seed(erlang:phash2(self()), Seconds, MicroSecs);
seed_if(false) ->
    no_op.

%% Returns N ± N*Percent. For example, `jitter(100, 0.1)` returns a number in
%% the interval [90, 110].
jitter(N, Percent) when 0.0 =< Percent andalso Percent =< 1.0 ->
    round(N * (1.0 - Percent + (random:uniform() * Percent * 2.0)));
jitter(_N, _Percent) ->
    throw({badarg, "Percent must be in the interval [0.0, 1.0]"}).

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

    ok = case gen_udp:open(0, [binary]) of
            {ok, Socket} ->
                 case gen_udp:send(Socket, ?STATSD_HOST, ?STATSD_PORT, StatsdMessage) of
                     ok ->
                         ok;
                     Error ->
                         {unable_to_send_to_statsd_socket, Error}
                 end;
            Error ->
                {unable_to_open_statsd_socket, Error}
         end.

remote_node() ->
    Hostname = case os:getenv("COMMITLOG_HOSTNAME") of
                   false -> [_Name, Host] = string:tokens(atom_to_list(node()), "@"),
                            Host;
                   EnvHost -> EnvHost
               end,
    Remote = "commitlog" ++ "@" ++ Hostname,
    list_to_atom(Remote).

