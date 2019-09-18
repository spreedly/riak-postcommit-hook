-module(postcommit_hook).
-export([send_to_kafka_riak_commitlog/1,
         send_to_commitlog_with_retries/3,
         retry_send_to_commitlog_if_retry_enabled/4,
         retry_send_to_commitlog/3,
         send_to_commitlog_timed/1,
         send_to_commitlog/1,
         call_commitlog/1,
         query_circuit_breaker_state/1,
         send_to_circuit_breaker/1,
         ensure_circuit_breaker_is_registered/0,
         start_circuit_breaker/0,
         stop_circuit_breaker/2,
         register_circuit_breaker/1,
         circuit_breaker_init/2,
         circuit_breaker_handle_call/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(COMMITLOG_PROCESS, kafka_riak_commitlog).

-define(CIRCUIT_BREAKER_TIMEOUT_MS, 1 * 60 * 1000).
-define(INITIAL_RETRY_DELAY_MS, 100).
-define(MAX_ATTEMPTS, 10).
-define(QUERY_TIMEOUT_MS, 1000).
-define(RETRY_DELAY_JITTER, 0.15).
-define(RETRY_DELAY_MULTIPLIER, 2.0).

-define(STATSD_HOST, "127.0.0.1").
-define(STATSD_PORT, 8125).


%% ---------------------------------------------------------------------------
%% API
%% ---------------------------------------------------------------------------

%% Sends a {produce, action, bucket, key, value} request to Commitlog if the
%% supplied Riak object is valid.
send_to_kafka_riak_commitlog(RiakObject) ->
    case build_commitlog_request(RiakObject) of
        {ok, Request} ->
            Call = {{?COMMITLOG_PROCESS, commitlog_node()}, Request},
            {MicroTime,
             {Result, Attempts}} = timer:tc(?MODULE, send_to_commitlog_with_retries,
                                            [Call, 0, ?INITIAL_RETRY_DELAY_MS]),
            Time = round(MicroTime / 1000),
            case Result of
                ok ->
                    log(info, "Send to Commitlog succeeded. Time: ~w ms. Attempts: ~w.",
                        [Time, Attempts], Call);
                {error, Error} ->
                    log(warn, "Send to Commitlog failed. Time: ~w ms. Attempts: ~w. Error: ~w.",
                        [Time, Attempts, Error], Call)
            end,
            Result;
        {error, RiakObjectError} ->
            log(warn, "Unable to extract data from Riak object '~w': ~w.",
                [RiakObject, RiakObjectError]),
            {error, RiakObjectError}
    end.

%% Sends a request to Commitlog. If the call fails, it will be retried using an
%% exponential backoff until it succeeds or all attempts fail. If all attempts
%% fail, a failure message is sent to the Circuit Breaker which will disable
%% retries until ?CIRCUIT_BREAKER_TIMEOUT_MS milliseconds have elapsed without
%% a subsequent failure.
%%
%% Retrying using an exponential backoff ensures that transient Commitlog
%% disruptions (e.g. due to deployment) do not result in failure.
%%
%% Disabling retries when multiple attempts to send to Commitlog fail ensures
%% that extended Commitlog disruptions (e.g. due to a downstream Kafka outage)
%% do not cause excessive back pressure on Riak (i.e. due to many postcommit
%% workers blocking during the exponential backoff retry process).
%%
%% When Commitlog is available again, calls will succeed without the need for
%% any retries, so the Circuit Breaker is shut down. If another attempt to send
%% fails, the Circuit Breaker will be restarted and retries will be disabled.
%%
%% The vast majority of the time, when Commitlog is fully available, the
%% Circuit Breaker does not run and retries are not needed. Short term
%% Commitlog disruptions are weathered by retries using exponential backoff. It
%% is only during extended Commitlog outages that the Circuit Breaker runs and
%% retries are temporarily disabled.
send_to_commitlog_with_retries(Call, NumAttempts, Delay) ->
    Result = ?MODULE:send_to_commitlog_timed(Call),
    Attempts = NumAttempts + 1,
    case Result of
        ok -> {ok, Attempts};
        {error, Error} ->
            log(info, "Unable to send to Commitlog. Attempts: ~w. Error: ~w.",
                [Attempts, Error], Call),
            case Attempts of
                % All attempts failed, return error
                ?MAX_ATTEMPTS -> send_to_circuit_breaker({result, failure}),
                                 {{error, Error}, Attempts};
                % First attempt failed
                1 -> seed_rng(),
                     ?MODULE:retry_send_to_commitlog_if_retry_enabled(Call, Attempts, Delay, Error);
                % Other attempt failed
                _ -> ?MODULE:retry_send_to_commitlog_if_retry_enabled(Call, Attempts, Delay, Error)
            end
    end.

%% Sends a request to Commitlog if retries are enabled; otherwise sends
%% a failure message to the Circuit Breaker which resets the timeout for
%% re-enabling retries.
retry_send_to_commitlog_if_retry_enabled(Call, NumAttempts, Delay, Error) ->
    case ?MODULE:query_circuit_breaker_state(?QUERY_TIMEOUT_MS) of
        {retry_enabled, _TTL} ->
            ?MODULE:retry_send_to_commitlog(Call, NumAttempts, Delay);
        _ -> %% retry_disabled or query_timeout or error
            send_to_circuit_breaker({result, failure}),
            {{error, Error}, NumAttempts}
    end.

%% Sends a request to Commitlog and sets the exponential backoff delay for the
%% next retry.
retry_send_to_commitlog(Call, NumAttempts, Delay) ->
    receive
    after
        Delay ->
            NextDelay = jitter(Delay * ?RETRY_DELAY_MULTIPLIER, ?RETRY_DELAY_JITTER),
            ?MODULE:send_to_commitlog_with_retries(Call, NumAttempts, NextDelay)
    end.

send_to_commitlog_timed(Call) ->
    {Time, Result} = timer:tc(?MODULE, send_to_commitlog, [Call]),
    send_timing_to_statsd(Time, Call),
    Result.

send_to_commitlog(Call) ->
    try ?MODULE:call_commitlog(Call)
    catch
        _:{Error, {gen_server, call, _}} -> {error, Error};
        _:Error -> {error, Error}
    end.

%% gen_server:call({kafka_riak_commitlog, 'commitlog@127.0.0.1'},
%%                 {produce, <<"store">>, <<"bucket">>, <<"key">>, <<"value">>}).
call_commitlog({ServerRef, Request}) ->
    gen_server:call(ServerRef, Request).

%% ---------------------------------------------------------------------------

%% Starts the Circuit Breaker if it is not running and returns its state.
query_circuit_breaker_state(Timeout) ->
    case ?MODULE:send_to_circuit_breaker({query, state}) of
        ok -> receive {circuit_breaker, {state, State}} -> State
              after Timeout -> query_timeout
              end;
        Error -> Error
    end.

%% Starts the Circuit Breaker if it is not running and sends it the supplied message.
send_to_circuit_breaker(Message) ->
    ?MODULE:ensure_circuit_breaker_is_registered(),
    try
        circuit_breaker ! {self(), Message},
        ok
    catch
        error:badarg -> {error, circuit_breaker_not_registered}
    end.

%% Starts the Circuit Breaker if it is not running. If multiple Circuit
%% Breakers are started due to a race condition, the redundant processes are
%% stopped.
ensure_circuit_breaker_is_registered() ->
    case whereis(circuit_breaker) of
        undefined ->
            Pid = ?MODULE:start_circuit_breaker(),
            case ?MODULE:register_circuit_breaker(Pid) of
                ok -> Pid;
                Error ->
                    ?MODULE:stop_circuit_breaker(Pid, Error),
                    ?MODULE:ensure_circuit_breaker_is_registered()
            end;
        Pid -> Pid
    end.

register_circuit_breaker(Pid) ->
    try
        register(circuit_breaker, Pid),
        log(info, "Name 'circuit_breaker' registered with ~w", [Pid]),
        ok
    catch
        error:badarg ->
            log(info, "Cannot register 'circuit_breaker' with ~w", [Pid]),
            {error, register_failed}
    end.

start_circuit_breaker() ->
    spawn(?MODULE, circuit_breaker_init, [self(), ?CIRCUIT_BREAKER_TIMEOUT_MS]).

stop_circuit_breaker(Pid, Reason) ->
    Pid ! {self(), {stop, Reason}}.

%% ---------------------------------------------------------------------------

%% Initializes the Circuit Breaker with retries enabled and a starts a timer
%% to shut it down in the event that no failures occur. Retries will remain
%% enabled until a failure message is received.
circuit_breaker_init(From, Timeout) ->
    cb_log("Circuit Breaker started by ~w; retries are enabled", [From]),
    circuit_breaker_start_timer(Timeout),
    State = {unknown, Timeout},
    circuit_breaker_handle_call(State).

%% The Circuit Breaker ensures that retries are disabled when a failure occurs
%% and remain disabled until the specified timeout has elapsed; it then
%% re-enables retries. Any time a new failure occurs, the timeout is reset.
%% After enabling retries, the Circuit Breaker shuts itself down.
%%
%% A timer is used to see if retries are enabled and, if so, to shut down the
%% Circuit Breaker. The timer does not need to be triggered to enable retries.
%% It is only used to ensure the Circuit Breaker terminates once it is no
%% longer needed. The Circuit Breaker will be restarted the next time a failure
%% occurs.
%%
%% LastFailureAt
%%     Time (in milliseconds) of the last known failure; initially `unknown`.
%% Timeout
%%     How long (in milliseconds) retries are disabled after a failure occurs.
circuit_breaker_handle_call(State={_LastFailureAt, Timeout}) ->
    receive
        {From, {query, state}} ->
            RetryState = circuit_breaker_retry_state(State),
            TTL = circuit_breaker_ttl(State),
            From ! {circuit_breaker, {state, {RetryState, TTL}}},
            ?MODULE:circuit_breaker_handle_call(State);

        {_From, {result, failure}} ->
            case circuit_breaker_ttl(State) of
                0 -> cb_log("Retries disabled for at least ~w s", [round(Timeout/1000)]);
                _ -> retry_disabled
            end,
            NewState = {timestamp_ms(), Timeout},
            ?MODULE:circuit_breaker_handle_call(NewState);

        {_From, timeout} ->
            case circuit_breaker_ttl(State) of
                0 ->
                    cb_log("Retries enabled", []),
                    self() ! {self(), {stop, timeout}},
                    ?MODULE:circuit_breaker_handle_call(State);
                TTL ->
                    %% Retries remain disabled; set a new timer that expires
                    %% when the last failure times out.
                    circuit_breaker_start_timer(TTL),
                    ?MODULE:circuit_breaker_handle_call(State)
            end;

        {From, {stop, Reason}=Message} ->
            cb_log("Circuit Breaker exiting: ~w", [Message]),
            exit({Reason, {from, From}})
    end.

circuit_breaker_retry_state(State) ->
    case circuit_breaker_ttl(State) of
        0 -> retry_enabled;
        _ -> retry_disabled
    end.

%% Returns the time remaining until retries are enabled.
circuit_breaker_ttl(_State={LastFailureAt, Timeout}) ->
    case LastFailureAt of
        unknown -> 0;
        _ ->
            ElapsedSinceLastFailure = timestamp_ms() - LastFailureAt,
            case ElapsedSinceLastFailure < Timeout of
                true -> Timeout - ElapsedSinceLastFailure;
                false -> 0
            end
    end.

circuit_breaker_start_timer(Delay) ->
    erlang:send_after(Delay, self(), {self(), timeout}).


%% ---------------------------------------------------------------------------
%% Internal
%% ---------------------------------------------------------------------------

-define(LOG_MSG_FMT,     "[postcommit-hook] ~s").
-define(LOG_MSG_FMT_B_K, "[postcommit-hook] ~s ~s|~s").

log(info, Format, Data) ->
    error_logger:info_msg(?LOG_MSG_FMT, [io_lib:format(Format, Data)]);
log(warn, Format, Data) ->
    error_logger:warning_msg(?LOG_MSG_FMT, [io_lib:format(Format, Data)]).

log(Level, Format, Data, Call) ->
    case Call of
        {_, {_, _, Bucket, Key, _}} -> log(Level, Format, Data, Bucket, Key);
        _ -> log(Level, Format, Data, unknown, unknown)
    end.

log(info, Format, Data, Bucket, Key) ->
    error_logger:info_msg(?LOG_MSG_FMT_B_K, [io_lib:format(Format, Data), Bucket, Key]);
log(warn, Format, Data, Bucket, Key) ->
    error_logger:warning_msg(?LOG_MSG_FMT_B_K, [io_lib:format(Format, Data), Bucket, Key]).

cb_log(Format, Data) ->
    error_logger:info_msg("[circuit-breaker] ~s", [io_lib:format(Format, Data)]).

%% Returns POSIX time in milliseconds
%%   https://gist.github.com/DimitryDushkin/5532071
timestamp_ms() ->
    {Mega, Sec, Micro} = os:timestamp(),
    (Mega*1000000 + Sec)*1000 + round(Micro/1000).

%% Seeding is necessary to avoid a race condition where multiple postcommit-
%% hook processes automatically seed the RNG in the same manner and all use the
%% same seed, which defeats the purpose of adding jitter.
seed_rng() ->
    {_, Seconds, MicroSecs} = now(),
    random:seed(erlang:phash2(self()), Seconds, MicroSecs).

%% Returns N ± N*Percent. For example, `jitter(100, 0.1)` returns a number in
%% the interval [90, 110].
jitter(N, Percent) when 0.0 =< Percent andalso Percent =< 1.0 ->
    round(N * (1.0 - Percent + (random:uniform() * Percent * 2.0)));
jitter(_N, _Percent) ->
    error({badarg, "Percent must be in the interval [0.0, 1.0]"}).

build_commitlog_request(RiakObject) ->
    try
        Action = get_action(RiakObject),
        Bucket = riak_object:bucket(RiakObject),
        Key    = riak_object:key(RiakObject),
        Value  = riak_object:get_value(RiakObject),
        {ok, {produce, Action, Bucket, Key, Value}}
    catch
        _:Error -> {error, Error}
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
        {error, Reason} = Error ->
            log(warn, "Unable to send timing to statsd: ~w.", [Reason], Call),
            Error
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

circuit_breaker_retry_state_test_() ->
    [{"Retuns retry_disabled when timeout has not expired ",
      fun() ->
              LastFailureAt = timestamp_ms(),
              Timeout = 100,
              ?assertEqual(retry_disabled,
                           circuit_breaker_retry_state({LastFailureAt, Timeout}))
      end},
     {"Retuns retry_enabled when timeout has expired",
      fun() ->
              LastFailureAt = timestamp_ms() - 200,
              Timeout = 100,
              ?assertEqual(retry_enabled,
                           circuit_breaker_retry_state({LastFailureAt, Timeout}))
      end}].

circuit_breaker_ttl_test_() ->
    [{"Retuns zero when last failure is unknown",
      fun() ->
              LastFailureAt = unknown,
              Timeout = 10000,
              ?assertEqual(0, circuit_breaker_ttl({LastFailureAt, Timeout}))
      end},
     {"Retuns zero when age of last failure >= timeout",
      fun() ->
              LastFailureAt = 100,
              Timeout = 10,
              ?assertEqual(0, circuit_breaker_ttl({LastFailureAt, Timeout}))
      end},
     {"Retuns milliseconds remaining until retries are enabled",
      fun() ->
              Now = timestamp_ms(),
              LastFailureAt = Now - 100,
              Timeout = 300,
              Expected = 200,
              Actual = circuit_breaker_ttl({LastFailureAt, Timeout}),
              %% Check that we're within ± 1 ms of the expected value so that
              %% this test doesn't fail due to timing issues.
              ?assert(Expected-1 =< Actual andalso Actual =< Expected+1)
      end}].

jitter_test_() ->
    [{"Returns N ± N*Percent.",
      fun() ->
              J = jitter(100, 0.1),
              ?assert(90 =< J andalso J =< 110)
      end},
     {"Returns error when Percent is < 0",
      fun() ->
              ?assertError({badarg, _}, jitter(1, -0.1))
      end},
     {"Returns error when Percent is > 1",
      fun() ->
              ?assertError({badarg, _}, jitter(1, 1.1))
      end}].

build_commitlog_request_test_() ->
    {foreach,
     fun() -> meck:new(riak_object, [non_strict]) end,
     fun(_) -> meck:unload(riak_object) end,
     [{"Returns {ok, Request} if object is valid",
       fun() ->
               meck:expect(riak_object, get_metadata, 1, dict:new()),
               meck:expect(riak_object, bucket, 1, b),
               meck:expect(riak_object, key, 1, k),
               meck:expect(riak_object, get_value, 1, v),
               ?assertEqual({ok, {produce, store, b, k, v}},
                            build_commitlog_request(a_riak_object)),
               ?assert(meck:validate(riak_object))
       end},
      {"Returns error if object is invalid",
       fun() ->
               meck:expect(riak_object, get_metadata, 1, nil),
               ?assertMatch({error, _}, build_commitlog_request(a_riak_object)),
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
     [{"Returns error if unable to send to statsd",
       fun() ->
               meck:expect(gen_udp, open, 2, {error, reason}),
               ?assertMatch({error, _}, send_timing_to_statsd(0, {})),
               ?assert(meck:validate(gen_udp))
       end},
      {"Returns ok if able to send to statsd",
       fun() ->
               meck:expect(gen_udp, open, 2, {ok, socket}),
               meck:expect(gen_udp, send, 4, ok),
               ?assertEqual(ok, send_timing_to_statsd(0, {})),
               ?assert(meck:validate(gen_udp))
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
