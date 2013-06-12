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
