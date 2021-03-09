EBIN_DIR=ebin
DEPS_DIR=deps
VSN = `grep vsn src/postcommit_hook.app.src | cut -d ',' -f 2 | grep -o "[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*"`
SHA = `git log -1 --format="%h"`

all: compile test

clean:
	rm -rf $(EBIN_DIR)

distclean: clean
	rm -rf $(DEPS_DIR)

$(DEPS_DIR):
	./rebar get-deps

compile: $(DEPS_DIR)
	@sed -i'.backup' -e 's/^{deps,.*/{deps, []}./' rebar.config
	./rebar compile
	@mv rebar.config.backup rebar.config

.PHONY:test
test:
	./rebar eunit

.PHONY:shell
shell: compile
	erl -pa ebin

.PHONY:install
install:
	cp ./ebin/postcommit_hook.beam $(HOME)/dev/dev-services/riak

.PHONY:uninstall
uninstall:
	rm $(HOME)/dev/dev-services/riak/postcommit_hook.beam

.PHONY:release
release: compile
	mkdir -p rel
	tar czvf rel/postcommit_hook.${VSN}-${SHA}.tar.gz -C ebin postcommit_hook.beam
	s3cmd put rel/postcommit_hook.${VSN}-${SHA}.tar.gz s3://spreedly-kafka-integration/postcommit_hook.${VSN}-${SHA}.tar.gz

.PHONY:ls-releases
ls-releases:
	s3cmd ls s3://spreedly-kafka-integration/postcommit_hook

.PHONY:help
help:
	@echo "If you're here to install the postcommit hook to your local dev-services Riak run"
	@echo "  make install"
	@echo ""
	@echo "Otherwise? Your available commands: clean, distclean, compile, test, install, release, help"
