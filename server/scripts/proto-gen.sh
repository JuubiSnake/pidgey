#!/usr/bin/env bash

# Find all directories containing at least one prototfile.
# Based on: https://buf.build/docs/migration-prototool#prototool-generate.
for dir in $(find ./protos -name '*.proto' -print0 | xargs -0 -n1 dirname | sort | uniq); do
  files=$(find protos -name '*.proto')

  # Generate all files with protoc-gen-go.
  protoc --go_out=generated --go_opt=paths=source_relative --go-grpc_out=generated --go-grpc_opt=paths=source_relative ${files}
done
