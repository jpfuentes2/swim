build:
	stack build --exec 'make fmt'

fmt:
	-hlint src/ test/
	-hindent --style cramer src/*.hs

devel:
	stack build --file-watch

.PHONY: build devel
