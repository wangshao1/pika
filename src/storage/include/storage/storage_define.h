//  Copyright (c) 2023-present The storage Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_DEFINE_H_
#define STORAGE_DEFINE_H_

namespace storage {
const int kPrefixReserveLength = 8;
const int kSUffixReserveLength = 16;

enum ColumnFamilyIndex {
    kStringsCF = 0,
    kHashesMetaCF = 1,
    kHashesDataCF = 2,
    kSetsMetaCF = 3,
    kSetsDataCF = 4,
    kListsMetaCF = 5,
    kListsDataCF = 6,
    kZsetsMetaCF = 7,
    kZsetsDataCF = 8,
    kZsetsScoreCF = 9,
};
} // end namespace storage
#endif
