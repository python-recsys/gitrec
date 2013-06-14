/*
 * Copyright 2013 Mortar Data Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

----------------------------------------------------------------------------------------------------

/*
 * All matrices are represented in Sparse COO format:
 * {row: int, col: int, val: float}
 *
 * We use floats instead of doubles because the github recommender does not
 * need numerical precision and it saves space when writing to disk inbetween MR jobs.
 *
 * If you plan to reuse these macros in a context where numerical precision is important,
 * you should change them to use doubles. If numerical precision is extremely important,
 * consider implementing the Kahan Summation Algorithm in a UDF.
 */

DEFINE MatrixProduct(A, B)
returns product {
    joined      =   JOIN $A BY col, $B BY row;
    terms       =   FOREACH joined GENERATE
                        $A::row AS row,
                        $B::col AS col,
                        $A::val * $B::val AS val;
    by_cell     =   GROUP terms BY (row, col);
    $product    =   FOREACH by_cell GENERATE
                        group.row AS row, group.col AS col,
                        (float) SUM(terms.val) AS val;
};

-- used to find shortest paths on a graph
-- http://en.wikipedia.org/wiki/Min-plus_matrix_multiplication

DEFINE MatrixMinPlusProduct(A, B)
returns product {
    joined      =   JOIN $A BY col, $B BY row;
    terms       =   FOREACH joined GENERATE
                        $A::row AS row,
                        $B::col AS col,
                        $A::val + $B::val AS val;
    by_cell     =   GROUP terms BY (row, col);
    $product    =   FOREACH by_cell GENERATE
                        group.row AS row, group.col AS col,
                        MIN(terms.val) AS val;
};

DEFINE MatrixSquared(M)
returns m_sq {
    copy    =   FOREACH $M GENERATE *;
    $m_sq   =   MatrixProduct($M, copy);
};

DEFINE MatrixMinPlusSquared(M)
returns m_sq {
    copy    =   FOREACH $M GENERATE *;
    $m_sq   =   MatrixMinPlusProduct($M, copy);  
};
