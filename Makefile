EBIN_DIR=ebin
DEPS_DIR=deps
CORE ?= $(HOME)/dev/core
ID ?= $(HOME)/dev/id
CORE_RIAK_BEAMS_DIR ?= $(CORE)/db/riak/$$n/tmp/beams
ID_RIAK_BEAMS_DIR ?= $(ID)/db/riak/$$n/tmp/beams
VSN = `grep vsn src/postcommit_hook.app.src | cut -d ',' -f 2 | grep -o "[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*"`
SHA = `git log -1 --format="%h"`
PKG_SRC = postcommit_hook.${VSN}-${SHA}.tar.gz
BRANCH = `git rev-parse --abbrev-ref HEAD`

all: compile

clean:
	rm -rf $(EBIN_DIR)

distclean: clean
	rm -rf $(DEPS_DIR)

$(DEPS_DIR):
	./rebar get-deps

compile: $(DEPS_DIR)
	./rebar compile

.PHONY:install
install:
	cp ./ebin/postcommit_hook.beam $(HOME)/dev/dev-services/riak

.PHONY:uninstall
uninstall:
	rm $(HOME)/dev/dev-services/postcommit_hook.beam

.PHONY:release
release: compile
	mkdir -p rel
	tar czvf rel/$(PKG_SRC) -C ebin postcommit_hook.beam
	s3cmd put rel/$(PKG_SRC) s3://spreedly-kafka-integration/$(PKG_SRC)

.PHONY:deploy
deploy: release
	PKG_SHA256=$$(shasum -a 256 rel/$(PKG_SRC) | cut -d' ' -f 1 ) ;\
	./deploy/notarize.sh riak-postcommit-hook $(BRANCH) $(PKG_SRC) $$PKG_SHA256

.PHONY:ls-releases
ls-releases:
	s3cmd ls s3://spreedly-kafka-integration/postcommit_hook

.PHONY:list
list:
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$' | xargs

.PHONY:help
help:
	@echo "If you're here to install the postcommit hook to your local Riak run"
	@echo "  ./post-commit-hooks copy-to-dev-services"
	@echo ""
	@echo "Otherwise? Your available commands: `make list`"
