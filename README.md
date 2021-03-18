# riak-postcommit-hook

An open-sourced post-commit hook for Riak that forwards Riak objects to an OTP application for handling. For more details on this, please see [this blog post](https://engineering.spreedly.com/blog/from-riak-to-kafka-part-1.html).

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Developer Installation](#developer-installation)
  - [Install Riak's required version of Erlang](#install-riaks-required-version-of-erlang)
  - [Compile a new version of the postcommit hook](#compile-a-new-version-of-the-postcommit-hook)
  - [Open a shell](#open-a-shell)
  - [Optional: Connect VS Code to elixir-build-machine](#optional-connect-vs-code-to-elixir-build-machine)
- [Run the postcommit hook in Riak](#run-the-postcommit-hook-in-riak)
  - [For the `dev-services` Riak VM](#for-the-dev-services-riak-vm)
- [Push a new release to S3](#push-a-new-release-to-s3)
- [Tests](#tests)
  - [Manually run all tests](#manually-run-all-tests)
  - [Manually run specific tests](#manually-run-specific-tests)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Developer Installation

### Install Riak's required version of Erlang

Riak requires included code to be compiled with Erlang version `R16B02-basho8`. The older versions of Erlang do not build on later versions of macOS, so it's likely easier to build in Linux -- for example, if you're at Spreedly, you can do this on the [`elixir-build-machine`](https://github.com/spreedly/elixir-build-machine) VM (otherwise you could use a stable version of Debian or Ubuntu Server in a VM or a container).

Follow the steps in http://docs.basho.com/riak/latest/ops/building/installing/erlang/ to get the correct Erlang version installed.

First, install [`kerl`](https://github.com/kerl/kerl) on the Debian/Ubuntu-based VM:

```
$ vagrant ssh elixir-build-machine
$ curl -O https://raw.githubusercontent.com/kerl/kerl/master/kerl
$ # Ensure it is executable
$ chmod a+x kerl
```

Next, build Riak's required version of Erlang:

```
$ ./kerl build git git://github.com/basho/otp.git OTP_R16B02_basho8 R16B02-basho8
```

Once Erlang is successfully built, install and activate the build:

```
$ ./kerl install R16B02-basho8 ~/erlang/R16B02-basho8
$ . ~/erlang/R16B02-basho8/activate
```

Verify the correct version is installed:

```
$ which erl
/home/vagrant/erlang/R16B02-basho8/bin/erl
```

### Compile a new version of the postcommit hook

Clone the RiakPostcommitHook repo onto the `elixir-build-machine` or your VM; if you're accessing the repository via public HTTPS update the git clone command accordingly (e.g. `git clone https://github.com/spreedly/riak-postcommit-hook.git`):

```
$ cd ~
$ git clone git@github.com:spreedly/riak-postcommit-hook.git
```

This project has a `make` configuration for compiling. Ensure that you're running the `R16B02` Erlang version and then run `make`.

**Note:** If you're attempting this on macOS, open `Makefile` and add a space between `@sed -i` and `'.backup'` in the `compile` function before running this step.

```
$ cd riak-postcommit-hook/
$ make
```

If you don't have the correct Erlang version active then `make` will throw an error instead of compiling with the incompatible version, e.g.

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

If you get this, please activate the correct Erlang version using `. ~/erlang/R16B02-basho8/activate` then try again.

### Open a shell

```
$ make shell
```

### Optional: Connect VS Code to elixir-build-machine

This step is optional for Spreedly members, and can be applied to use VS Code with other VMs.

You can already use an editor like vim to edit code on `elixir-build-machine`.

You can also use VS Code to edit RiakPostcommitHook on `elixir-build-machine` if you'd like.

Assuming you've already set up `elixir-build-machine`, and can SSH into it:

1. In VS Code, install the `Remote-SSH` extension.
1. In the bottom left corner of VS Code, click the two small arrows on a green background, and then select `Remote-ssh: Connect to host`.
1. Select `elixir-build-machine`.
1. Hit Cmd+Option+E; this should bring up an "Open Folder" button.
1. Click "Open Folder" and choose the folder you want to edit in VS Code.

## Run the postcommit hook in Riak

You'll need a running Riak instance configured with an added path for custom BEAM files (such as Spreedly's `dev-services` Riak VM). Place the BEAM file in the configured path in Riak and then configure buckets to call the Erlang module/function: `postcommit_hook:send_to_kafka_riak_commitlog`.

### For the `dev-services` Riak VM

1. Copy the new beam file from the build machine to your local machine:

```
$ cd ~/dev/dev-services
$ scp elixir-build-machine:riak-postcommit-hook/ebin/postcommit_hook.beam riak/
```

2. Restart Riak

```
$ docker-compose stop riak
$ docker-compose start riak
```

## Push a new release to S3

Make sure that you've incremented the `vsn` in `src/postcommit_hook.app.src`. That's used to name the folder up on S3 that will hold the new release.

If you haven't already installed it you'll need to install and configure `s3cmd`:

```
$ brew install s3cmd
$ s3cmd --configure # use the "kafka-integration S3 credentials" from the 1Password "Engineering" vault
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
