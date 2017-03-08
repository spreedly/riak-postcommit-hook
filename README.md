# riak-postcommit-hook

A postcommit hook for Riak that forwards Riak objects to an OTP application for handling.

## Developer Installation

### Install Riak's required version of Erlang

Riak requires included code to be compiled with `Erlang R16B02-basho8`. Follow the steps in http://docs.basho.com/riak/latest/ops/building/installing/erlang/ to get the correct Erlang version installed.

After installation you'll be able to activate and deactivate the Riak version of Erlang.

```
$HOME/erlang/R16B02-basho8/activate
$HOME/erlang/R16B02-basho8/deactivate
```

I suggest making aliases for those commands so switching Erlang versions is easy.

If you don't have the correct Erlang version active then `make` will throw an error instead of compiling with the incompatible version.

e.g.

```bash
$ make
./rebar get-deps
==> riak-postcommit-hook (get-deps)
./rebar compile
==> riak-postcommit-hook (compile)
ERROR: OTP release 18 does not match required regex R16B02_basho8
ERROR: compile failed while processing /Users/sdball/github/spreedly/riak-postcommit-hook: rebar_abort
make: *** [compile] Error 1
```

### Compile a new version of the postcommit hook

This project has a `make` configuration for compiling. You only need to ensure that your running the `R16B02` Erlang version and then run make.

```
$ make
```

### Run the postcommit hook in Riak

You'll need a running Riak instance configured with an added path for custom BEAM files (such as our dev-services Riak VM). Place the BEAM file in the configured path in Riak and then configure buckets to call the Erlang module/function: `postcommit_hook:send_to_kafka_riak_commitlog`.

#### For the dev-services Riak VM:

1. Copy the newly compiled `./ebin/postcommit_hook.beam` file to `dev-services/riak/postcommit_hook.beam`
    - `make install` will do this for you if dev-services is located at `~/dev/dev-services`
2. Run `vagrant provision riak` in the dev-services directory

The dev-services Riak provisioning scripts will place the beam file in the proper location and restart Riak. The dev-services Riak is already configured to call this postcommit hook with data writes to any bucket.

### Push a new release to S3

Make sure that you've incremented the VSN in src/postcommit_hook.app.src. That's used to name the folder up on S3 that will hold the new release.

If you haven't already installed it you'll need to install and configure s3cmd:

```
$ brew install s3cmd
$ s3cmd --configure # use the "kafka-integration S3 credentials" from the 1Password dev vault
```

```
$ make release # compiles, tars up the release, pushes to S3
```

