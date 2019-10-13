// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/dataset/dataset.h"

#include <memory>
#include <utility>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/util/iterator.h"
#include "arrow/util/stl.h"

namespace arrow {
namespace dataset {

DataFragment::DataFragment() : scan_options_(ScanOptions::Defaults()) {}

SimpleDataFragment::SimpleDataFragment(
    std::vector<std::shared_ptr<RecordBatch>> record_batches,
    std::shared_ptr<ScanOptions> scan_options)
    : DataFragment(std::move(scan_options)), record_batches_(std::move(record_batches)) {}

SimpleDataFragment::SimpleDataFragment(
    std::vector<std::shared_ptr<RecordBatch>> record_batches)
    : SimpleDataFragment(std::move(record_batches), ScanOptions::Defaults()) {}

Status SimpleDataFragment::Scan(std::shared_ptr<ScanContext> scan_context,
                                ScanTaskIterator* out) {
  // Make an explicit copy of record_batches_ to ensure Scan can be called
  // multiple times.
  auto batches_it = MakeVectorIterator(record_batches_);

  // RecordBatch -> ScanTask
  auto scan_options = scan_options_;
  auto fn = [=](std::shared_ptr<RecordBatch> batch) -> std::unique_ptr<ScanTask> {
    std::vector<std::shared_ptr<RecordBatch>> batches{batch};
    return ::arrow::internal::make_unique<SimpleScanTask>(
        std::move(batches), std::move(scan_options), std::move(scan_context));
  };

  *out = MakeMapIterator(fn, std::move(batches_it));
  return Status::OK();
}

Status Dataset::Make(std::vector<std::shared_ptr<DataSource>> sources,
                     std::shared_ptr<Schema> schema, std::shared_ptr<Dataset>* out) {
  // TODO: Ensure schema and sources align.
  *out = std::make_shared<Dataset>(std::move(sources), std::move(schema));

  return Status::OK();
}

Status Dataset::NewScan(std::unique_ptr<ScannerBuilder>* out) {
  auto context = std::make_shared<ScanContext>();
  out->reset(new ScannerBuilder(this->shared_from_this(), context));
  return Status::OK();
}

bool DataSource::AssumePartitionExpression(
    const std::shared_ptr<ScanOptions>& scan_options,
    std::shared_ptr<ScanOptions>* simplified_scan_options) const {
  if (partition_expression_ == nullptr) {
    if (simplified_scan_options != nullptr) {
      *simplified_scan_options = scan_options;
    }
    return true;
  }

  auto expr = scan_options->filter->Assume(*partition_expression_);
  if (expr->IsNull() || expr->Equals(false)) {
    // selector is not satisfiable; yield no fragments
    return false;
  }

  if (simplified_scan_options != nullptr) {
    auto copy = std::make_shared<ScanOptions>(*scan_options);
    copy->filter = std::move(expr);
    *simplified_scan_options = std::move(copy);
  }
  return true;
}

DataFragmentIterator DataSource::GetFragments(std::shared_ptr<ScanOptions> scan_options) {
  std::shared_ptr<ScanOptions> simplified_scan_options;
  if (!AssumePartitionExpression(scan_options, &simplified_scan_options)) {
    return MakeEmptyIterator<std::shared_ptr<DataFragment>>();
  }
  return GetFragmentsImpl(std::move(simplified_scan_options));
}

DataFragmentIterator SimpleDataSource::GetFragmentsImpl(
    std::shared_ptr<ScanOptions> scan_options) {
  return MakeVectorIterator(fragments_);
}

DataFragmentIterator TreeDataSource::GetFragmentsImpl(
    std::shared_ptr<ScanOptions> options) {
  return GetFragmentsFromSources(children_, options);
}

}  // namespace dataset
}  // namespace arrow
