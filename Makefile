kernelDir = $(HOME)/.local/share/jupyter/kernels/clojure
unreplKernelDir = $(HOME)/.local/share/jupyter/kernels/remote-clojure

ifeq ($(shell uname -s), Linux)
        kernelDir:=$(HOME)/.local/share/jupyter/kernels/clojure
        unreplKernelDir:=$(HOME)/.local/share/jupyter/kernels/remote-clojure
endif

ifeq ($(shell uname -s), Darwin)
        kernelDir:=$(HOME)/Library/Jupyter/kernels/clojure
        unreplKernelDir:=$(HOME)/Library/Jupyter/kernels/remote-clojure
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
	sed 's|KERNEL|'${kernelDir}/clojupyter'|;s|TYPE|nrepl|' resources/kernel.json > $(kernelDir)/kernel.json;\
	mkdir -p $(unreplKernelDir)
	cp bin/clojupyter $(unreplKernelDir)/clojupyter
	sed 's|KERNEL|'${unreplKernelDir}/clojupyter'|;s|TYPE|unrepl|;s|Clojure|Clojure (remote)|;s|"clojure"|"iclojure"|' resources/kernel.json > $(unreplKernelDir)/kernel.json;\
