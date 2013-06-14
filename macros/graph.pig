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

-- To find shortest paths on a graph using Min-Plus Matrix Multiplication
-- (see matrix.pig), you need to first add 0-weight self loops to each vertex
-- (and then remove them when you are done with the pathfinding).

DEFINE AddSelfLoops(mat)
returns out_mat, vertices {
    from_vertices       =   FOREACH $mat GENERATE row AS id;
    to_vertices         =   FOREACH $mat GENERATE col AS id;
    vertices_with_dups  =   UNION from_vertices, to_vertices;
    $vertices           =   DISTINCT vertices_with_dups;
    self_loops          =   FOREACH $vertices GENERATE id AS row, id AS col, 0.0f AS val: float;
    $out_mat            =   UNION self_loops, $mat;
};
