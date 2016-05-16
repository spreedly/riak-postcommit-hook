# riak-postcommit-hook

A postcommit hook for Riak that forwards Riak objects to an OTP application for handling.

## Developer Installation

### Install Riak's required version of Erlang

Riak requires included code to be compiled with `Erlang R16B02-basho8`. Follow
the steps in http://docs.basho.com/riak/latest/ops/building/installing/erlang/
to get the correct Erlang version installed.

After installation you'll be able to activate and deactivate the Riak version of Erlang.

```
$HOME/erlang/R16B02-basho8/activate
$HOME/erlang/R16B02-basho8/deactivate
```

I suggest making aliases for those commands so switching Erlang versions is easy.

If you don't have the correct Erlang version active then `make` will throw an
error instead of compiling with the incompatible version.

e.g.

```bash
./rebar get-deps
==> kafka-riak-commitlog (get-deps)
./rebar compile
==> kafka-riak-commitlog (compile)
ERROR: OTP release 18 does not match required regex R16B02_basho8
ERROR: compile failed while processing /Users/sdball/github/spreedly/kafka-riak-commitlog: rebar_abort
make: *** [compile] Error 1
```

### Compile and install the post-commit hook

```
$ make
$ make install
```

### Check that the code is in place

```
$ make ls-install
```

### Configure riak to call the post-commit hook

```
$ ./post-commit-hooks add
```

### Restart local riak nodes

```
$ ./post-commit-hooks rolling-restart-riak
```

Done! At this point any commits to your local core or id riak nodes should be
calling the postcommit hook which will in turn try to forward the Riak objects
to a separate OTP application.
