# Build the Python-process container used by @pytest.mark.memory_container.
#
# Invoke from this directory:
#     make image-build          # build the soda-memtest image
#     make image-clean          # remove it
#     make image-info           # show id/size or a hint if not built
#
# Override the tag at the command line:
#     make image-build IMAGE=soda-memtest:dev

IMAGE ?= soda-memtest:latest
DOCKERFILE := Dockerfile
CONTEXT := .

.PHONY: image-build image-clean image-info

image-build:
	docker build -t $(IMAGE) -f $(DOCKERFILE) $(CONTEXT)

image-clean:
	-docker rmi $(IMAGE)

image-info:
	@docker image inspect $(IMAGE) --format '{{.Id}} {{.Size}} bytes' 2>/dev/null \
		|| echo "image $(IMAGE) not built — run: make image-build"
