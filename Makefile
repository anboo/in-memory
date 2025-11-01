FLATC_IMAGE = docker.io/flatbuffers/flatc
SCHEMA = rpc.fbs
OUT_DIR = generated

.PHONY: generate
generate:
	docker run --rm \
		-v $(PWD):/work \
		-w /work \
		$(FLATC_IMAGE) \
		flatc --go -o $(OUT_DIR) $(SCHEMA)
