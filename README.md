# riak-postcommit-hook

A postcommit hook for Riak that forwards Riak objects to an OTP application for handling.

## Developer Installation

### Install Riak's required version of Erlang

Riak requires included code to be compiled with `Erlang R16B02-basho8`.
Follow the steps in
http://docs.basho.com/riak/latest/ops/building/installing/erlang/ to get
the correct Erlang version installed. The older version of Erlang do not
build on later versions of macOS, so it's likely easier to build on our
`elixir-build-machine`.

```
$ vagrant ssh elixir-build-machine
$ ./kerl install R16B02-basho8 ~/erlang/R16B02-basho8
$ git clone git@github.com:spreedly/riak-postcommit-hook.git
```

After installation you'll be able to activate and deactivate the Riak version of Erlang.

```
. ~/erlang/R16B02-basho8/activate
```

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

1. Copy the new beam file from the build machine to your local machine:

```
$ pwd
.../dev/dev-services
$ scp elixir-build-machine:riak-postcommit-hook/ebin/postcommit_hook.beam riak
```

2. Copy the beam file to the Riak directory on the `core` VM:

```
$ sudo cp /vagrant/riak/postcommit_hook.beam /opt/beams/
```

3. Restart Riak

```
$ sudo riak stop
$ sudo riak start
```

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


## Tests

Tests are run automatically when you compile the project using `make`.

### Manually run all tests

To run all the tests (without compiling the project):

```
$ make test
```

### Manually run specific tests

To run specific tests:

```
$ ./rebar eunit tests=postcommit_hook:TEST_NAME

# For example:
#   ./rebar eunit tests=postcommit_hook:call_commitlog_test_
```
