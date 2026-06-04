# Build the Python-process container used by @pytest.mark.memory_container.
#
# Invoke from this directory:
#     make image-build          # build the soda-memtest image
#     make image-clean          # remove it
#     make image-info           # show id/size or a hint if not built
#
# Override the tag at the command line:
#     make image-build IMAGE=soda-memtest:dev
#
# Build context is the soda_claude parent (../../../..) so we can COPY both
# soda-core/ and soda-extensions/ into the image. .dockerignore in this
# directory is passed via --ignore-file so colleagues get the same exclude
# rules (Docker reads .dockerignore from the build context root by default,
# but our build context is the parent which isn't a tracked repo).

IMAGE ?= soda-memtest:latest
DOCKERFILE := Dockerfile
IGNORE_FILE := .dockerignore
BUILD_CTX := ../../..
REPO_PARENT_ARG ?= /Users/niels/Repositories/soda_claude

.PHONY: image-build image-clean image-info

image-build:
	@( \
		set -e ; \
		cp $(IGNORE_FILE) $(BUILD_CTX)/.dockerignore ; \
		trap "rm -f $(BUILD_CTX)/.dockerignore" EXIT ; \
		docker buildx build \
			-t $(IMAGE) \
			-f $(DOCKERFILE) \
			--build-arg REPO_PARENT=$(REPO_PARENT_ARG) \
			--load \
			$(BUILD_CTX) ; \
	)

image-clean:
	-docker rmi $(IMAGE)

image-info:
	@docker image inspect $(IMAGE) --format '{{.Id}} {{.Size}} bytes' 2>/dev/null \
		|| echo "image $(IMAGE) not built — run: make image-build"
