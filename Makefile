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
	@for n in 1 2 3 4; do \
		find . -iname "*.beam" -exec cp {} $(CORE_RIAK_BEAMS_DIR) \; ; \
		find . -iname "*.beam" -exec cp {} $(ID_RIAK_BEAMS_DIR) \; ; \
		$(CORE)/db/riak/$$n/bin/riak-admin erl_reload > /dev/null \; ; \
		$(ID)/db/riak/$$n/bin/riak-admin erl_reload > /dev/null \; ; \
	done
	@echo "Post-commit code copied to id and core."

.PHONY:uninstall
uninstall:
	@for n in 1 2 3 4; do \
		rm -rf $(CORE_RIAK_BEAMS_DIR)/*; \
		rm -rf $(ID_RIAK_BEAMS_DIR)/*; \
	done
	@echo "Post-commit code deleted from id and core."

.PHONY:ls-install
ls-install:
	@for n in 1 2 3 4; do \
		echo; \
		find $(CORE_RIAK_BEAMS_DIR)/*; \
		find $(ID_RIAK_BEAMS_DIR)/*; \
	done

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
	@echo "You probably want to do something like this:"
	@echo "  1. Compile the post-commit hook:               make"
	@echo "  2. Copy the compiled code to id and core:      make install"
	@echo "  3. Set post-commit hook in id and core riak:   ./post-commit-hooks add"
	@echo ""
	@echo "Available commands:" `make list`
