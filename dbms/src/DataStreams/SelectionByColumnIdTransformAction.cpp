// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <DataStreams/SelectionByColumnIdTransformAction.h>

#include <Core/ColumnShreddedAttachment.h>

namespace DB
{
SelectionByColumnIdTransformAction::SelectionByColumnIdTransformAction(
    const Block & in_header_,
    const Block & out_header_)
    : out_header(out_header_)
{
    col_offset_by_id.reserve(in_header_.columns());
    for (size_t col_index = 0; col_index < in_header_.columns(); ++col_index)
    {
        const auto & c = in_header_.getByPosition(col_index);
        col_offset_by_id[c.column_id] = col_index;
    }
}

Block SelectionByColumnIdTransformAction::transform(const Block & in_block)
{
    Block new_block;
    for (const auto & c : out_header)
    {
        auto col_to_trans = in_block.getByPosition(col_offset_by_id.at(c.column_id));
        col_to_trans.name = c.name;
        new_block.insert(std::move(col_to_trans));
    }
    return new_block;
}

Block SelectionByColumnIdTransformAction::filterAndTransform(
    const Block & in_block,
    const IColumn::Filter & filter,
    ssize_t result_size_hint)
{
    Block new_block;
    for (const auto & c : out_header)
    {
        auto col_to_trans = in_block.getByPosition(col_offset_by_id.at(c.column_id));
        col_to_trans.name = c.name;
        col_to_trans.column = col_to_trans.column->filter(filter, result_size_hint);

        // When this column has a shredded sidecar attachment AND MVCC filtering reduced
        // the row count, attach the filter bitmap so FunctionsJson can select the correct
        // sidecar rows. The attachment's row_count reflects the unfiltered pack size; the
        // mvcc_filter tells FunctionsJson which of those rows actually survived.
        if (col_to_trans.shredded_attachment && !col_to_trans.shredded_attachment->is_ngc
            && !filter.empty()
            && col_to_trans.column->size() != col_to_trans.shredded_attachment->row_count)
        {
            auto new_attach = std::make_shared<DM::ColumnShreddedAttachment>(*col_to_trans.shredded_attachment);
            new_attach->mvcc_filter.assign(filter.begin(), filter.end());
            col_to_trans.shredded_attachment = std::move(new_attach);
        }

        new_block.insert(std::move(col_to_trans));
    }
    return new_block;
}

} // namespace DB
