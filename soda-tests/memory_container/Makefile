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
# Build context is the workspace parent (../../..) so we can COPY both
# soda-core/ and soda-extensions/ into the image. .dockerignore in this
# directory is passed via --ignore-file so colleagues get the same exclude
# rules (Docker reads .dockerignore from the build context root by default,
# but our build context is the parent which isn't a tracked repo).

IMAGE ?= soda-memtest:latest
DOCKERFILE := Dockerfile
IGNORE_FILE := .dockerignore
BUILD_CTX := ../../..
# Absolute path of the build context (the workspace parent), derived from THIS
# Makefile's own location so it is correct for any checkout and regardless of the
# working directory (e.g. `make -C`). The image COPYs the repos to this absolute path
# and registers editable installs there; it must match the runtime bind-mount path the
# plugin derives from its own location (memory_container_plugin.REPO_PARENT). Override
# explicitly if needed:  make image-build REPO_PARENT_ARG=/your/path
REPO_PARENT_ARG ?= $(abspath $(dir $(lastword $(MAKEFILE_LIST)))$(BUILD_CTX))

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
