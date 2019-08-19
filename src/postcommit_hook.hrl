-define(COMMITLOG_PROCESS, kafka_riak_commitlog).

-define(INITIAL_RETRY_DELAY_MS, 100).
-define(MAX_ATTEMPTS, 6).
-define(RETRY_DELAY_JITTER, 0.15).
-define(RETRY_DELAY_MULTIPLIER, 2.0).

-define(STATSD_HOST, "127.0.0.1").
-define(STATSD_PORT, 8125).
