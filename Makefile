REBAR = rebar3
COVERPATH = $(shell pwd)/_build/test/cover
#.PHONY: rel deps test

all: compile

compile: 
	$(REBAR) compile

#deps:
#	$(REBAR) get-deps

clean:
	$(REBAR) clean

distclean: clean relclean
	$(REBAR) clean -all

cleantests:
	rm -f test/utils/*.beam
	rm -f test/singledc/*.beam
	rm -f test/multidc/*.beam
	rm -rf logs/

rel: all
	$(REBAR) release

relclean:
	rm -rf _build/default/rel

reltest: rel
	test/release_test.sh

compile-utils: compile
	for filename in "test/*.erl" ; do \
		erlc -o test/ $$filename ; \
	done

test:
	${REBAR} eunit

coverage:
	# copy the coverdata files with a wildcard filter
	# won't work if there are multiple folders (multiple systests)
	cp logs/*/*singledc*/../all.coverdata ${COVERPATH}/singledc.coverdata ; \
	cp logs/*/*multidc*/../all.coverdata ${COVERPATH}/multidc.coverdata ; \
	${REBAR} cover --verbose

commontest: compile-utils rel
	rm -f test/*.beam
	mkdir -p logs
ifdef SUITE
	ct_run -pa ./_build/default/lib/*/ebin test/ -logdir logs -suite test/${SUITE}
else
	ct_run -pa ./_build/default/lib/*/ebin test/ -logdir logs -dir test/
endif

multidc: compile-utils rel
	rm -f test/multidc/*.beam
	mkdir -p logs
ifdef SUITE
	ct_run -pa ./_build/default/lib/*/ebin test/utils/ -logdir logs -suite test/multidc/${SUITE} -cover test/antidote.coverspec
else
	ct_run -pa ./_build/default/lib/*/ebin test/utils/ -logdir logs -dir test/multidc -cover test/antidote.coverspec
endif

systests: singledc multidc

#relnocert: export NO_CERTIFICATION = true
#relnocert: relclean cleantests rel

#stage : rel
#	$(foreach dep,$(wildcard deps/*), rm -rf rel/saturn_leaf/lib/$(shell basename $(dep))-* && ln -sf $(abspath $(dep)) rel/saturn_leaf/lib;)
#	$(foreach app,$(wildcard apps/*), rm -rf rel/saturn_leaf/lib/$(shell basename $(app))-* && ln -sf $(abspath $(app)) rel/saturn_leaf/lib;)

#currentdevrel: stagedevrel compile-riak-test
#	riak_test/bin/saturnleaf-current.sh

#riak-test: currentdevrel
#	$(foreach dep,$(wildcard riak_test/*.erl), ../riak_test/riak_test -v -c saturn_leaf -t $(dep);)

#stage-riak-test: all
#	$(foreach dep,$(wildcard riak_test/*.erl), ../riak_test/riak_test -v -c saturn_leaf -t $(dep);)

##
## Developer targets
##
##  devN - Make a dev build for node N
##  stagedevN - Make a stage dev build for node N (symlink libraries)
##  devrel - Make a dev build for 1..$DEVNODES
##  stagedevrel Make a stagedev build for 1..$DEVNODES
##
##  Example, make a 68 node devrel cluster
##    make stagedevrel DEVNODES=68

#.PHONY : stagedevrel devrel

#DEVNODES ?= 5

# 'seq' is not available on all *BSD, so using an alternate in awk
#SEQ = $(shell awk 'BEGIN { for (i = 1; i < '$(DEVNODES)'; i++) printf("%i ", i); print i ;exit(0);}')

#$(eval stagedevrel : $(foreach n,$(SEQ),stagedev$(n)))
#$(eval devrel : $(foreach n,$(SEQ),dev$(n)))

#dev% : all
#	mkdir -p dev
#	rel/gen_dev $@ rel/vars/dev_vars.config.src rel/vars/$@_vars.config
#	(cd rel && $(REBAR) generate target_dir=../dev/$@ overlay_vars=vars/$@_vars.config)

#stagedev% : dev%
#	  $(foreach dep,$(wildcard deps/*), rm -rf dev/$^/lib/$(shell basename $(dep))* && ln -sf $(abspath $(dep)) dev/$^/lib;)
#	  $(foreach app,$(wildcard apps/*), rm -rf dev/$^/lib/$(shell basename $(app))* && ln -sf $(abspath $(app)) dev/$^/lib;)

#devclean: clean
#	rm -rf dev

#DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
#	xmerl webtool eunit syntax_tools compiler mnesia public_key snmp

#include tools.mk

#typer:
#	typer --annotate -I ../ --plt $(PLT) -r src
