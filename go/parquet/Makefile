# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GO_BUILD=go build
GO_GEN=go generate
GO_TEST?=go test
GOPATH=$(realpath ../../../../../..)

GO_SOURCES  := $(shell find . -path ./_lib -prune -o -name '*.go' -not -name '*_test.go')
ALL_SOURCES := $(shell find . -path ./_lib -prune -o -name '*.go' -name '*.s' -not -name '*_test.go')
SOURCES_NO_VENDOR := $(shell find . -path ./vendor -prune -o -name "*.go" -not -name '*_test.go' -print)

.PHONEY: generate

generate: bin/tmpl
	# bin/tmpl -i -data=numeric.tmpldata parquet/arrow/scalar.gen.go.tmpl
	# bin/tmpl -i -data=parquet/arrow/scalar.json parquet/arrow/scalar.gen.go.tmpl

bin/tmpl: _tools/tmpl/main.go
	$(GO_BUILD) -o $@ ./_tools/tmpl

