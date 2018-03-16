kernelDir = $(HOME)/.local/share/jupyter/kernels/iclojure

ifeq ($(shell uname -s), Linux)
        kernelDir:=$(HOME)/.local/share/jupyter/kernels/iclojure
endif

ifeq ($(shell uname -s), Darwin)
        kernelDir:=$(HOME)/Library/Jupyter/kernels/iclojure
endif

all:
	lein uberjar
	cat bin/clojupyter.template $$(find . -maxdepth 2 -type f | grep -e ".*standalone.*\.jar") > bin/clojupyter
	chmod +x bin/clojupyter

clean:
	rm -f *.jar
	rm -f target/*.jar
	rm -f bin/clojuypyter

install:
	mkdir -p $(kernelDir)
	cp bin/clojupyter $(kernelDir)/clojupyter
	sed 's|KERNEL|'${kernelDir}/clojupyter'|' resources/kernel.json > $(kernelDir)/kernel.json;\
