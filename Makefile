EBIN_DIR=ebin
DEPS_DIR=deps
CORE ?= $(HOME)/dev/core
ID ?= $(HOME)/dev/id
CORE_RIAK_BEAMS_DIR ?= $(CORE)/db/riak/$$n/tmp/beams
ID_RIAK_BEAMS_DIR ?= $(ID)/db/riak/$$n/tmp/beams

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

.PHONY:help
help:
	@echo "You probably want to do something like this:"
	@echo "  1. Compile the post-commit hook:               make"
	@echo "  2. Copy the compiled code to id and core:      make install"
	@echo "  3. Set post-commit hook in id and core riak:   ./post-commit-hooks add"
	@echo "  4. (Re)load the ERL code available to riak:    ./post-commit-hooks riak-erl-reload"
	@echo ""
	@echo "Available commands: clean, distclean, compile, install, ls-install, help"
