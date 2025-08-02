.PHONY: generate

generate:
	@mkdir -p environment/
	python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. api/environment.proto
