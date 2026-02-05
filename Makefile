NAME:=observation-encoder

#######################################
# VERSION SOURCE OF TRUTH FOR PROJECT #
#######################################
VERSION:=0.0.0

OUT:=./out
DEFAULT_INSTALLDIR:=/usr/bin
INSTALL:=install -p -m 0755
COMMIT:=$$(cat COMMIT 2> /dev/null || git describe --dirty=+WiP --always 2> /dev/null)

.PHONY: build install clean fmt vet coverage


all: build

build: outdir
	go build -v -ldflags "-X 'main.version=$(VERSION)' -X 'main.commit=$(COMMIT)'" -o $(OUT)/ ./cmd/...

outdir:
	-mkdir -p $(OUT)

clean:
	-rm -rf $(OUT)

install:
	test -z "$(DESTDIR)" && $(INSTALL) $(OUT)/$(PROG) $(DEFAULT_INSTALLDIR) || $(INSTALL) $(OUT)/$(PROG) $(DESTDIR)$(prefix)/bin/

test:
	go test ./...

coverage: outdir
	go test -coverprofile=$(OUT)/coverage.out ./...
	go tool cover -html="$(OUT)/coverage.out" -o $(OUT)/coverage.html

fmt:
	go fmt ./...

vet:
	go vet ./...

tarball: outdir
	@test ! -f COMMIT || (echo "Trying to make tarball from extracted tarball?" && false)
	@test ! -z $(COMMIT) || (echo "Not tracked by git?" && false)
	@test -z $$(git status --porcelain) || (echo "won't make tarball from dirty history" && false)
	echo "$(COMMIT)" > $(OUT)/COMMIT
	git archive --format=tar.gz --prefix=$(NAME)/ -o $(OUT)/$(NAME)-$(VERSION).tar.gz --add-file $(OUT)/COMMIT HEAD
	cd $(OUT) && sha256sum -b $(NAME)-$(VERSION).tar.gz > $(NAME)-$(VERSION).sha256.txt
