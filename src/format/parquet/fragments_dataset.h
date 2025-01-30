#pragma once

#include <arrow/dataset/dataset.h>
#include <arrow/util/iterator.h>

namespace pond::format {

class FragmentsDataset : public arrow::dataset::Dataset {
public:
    using FragmentVector = std::vector<std::shared_ptr<arrow::dataset::Fragment>>;

    FragmentsDataset(std::shared_ptr<arrow::Schema> schema, FragmentVector fragments)
        : arrow::dataset::Dataset(schema), fragments_(std::move(fragments)) {}

    std::string type_name() const override { return "FragmentsDataset"; }

    arrow::Result<std::shared_ptr<arrow::dataset::Dataset>> ReplaceSchema(
        std::shared_ptr<arrow::Schema> schema) const override {
        return arrow::Status::NotImplemented("ReplaceSchema is not implemented");
    }

protected:
    virtual arrow::Result<arrow::dataset::FragmentIterator> GetFragmentsImpl(
        arrow::compute::Expression predicate) override {
        // TODO: run predictes on the framents and return the filtered fragments
        return arrow::Result(arrow::MakeVectorIterator(fragments_));
    }

private:
    FragmentVector fragments_;
};
}  // namespace pond::format
