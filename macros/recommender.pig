REGISTER 's3n://mhc-software-mirror/datafu/datafu-0.0.9-SNAPSHOT.jar';
DEFINE EnumerateFromZero datafu.pig.bags.Enumerate('0');
DEFINE EnumerateFromOne datafu.pig.bags.Enumerate('1');

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

/*
 * These macros that do most of the heavy lifting for the github recommender.
 */

/*
 * events: {anything as long as it has the field entity_field refers to}
 * entity_field: name of the field to assign ids to (passed to macro as a string)
 * -->
 * id_map: {id, name}
 */
DEFINE AssignIntegerIds(events, entity_field)
returns id_map {
    grouped             =   GROUP $events BY $entity_field;
    names               =   FOREACH grouped GENERATE group AS name;
    enumerated          =   FOREACH (GROUP names ALL) GENERATE
                                FLATTEN(EnumerateFromOne(names))
                                AS (name, id);
    $id_map             =   FOREACH enumerated GENERATE id, name;
};

/*
 * events: {user, item, type[, timestamp]}
 * valuation_udf: name of a udf, i.e. 'my_py_udfs.valuation' or 'my.java.package.Valuation'
 *                that takes the event type and returns a tuple with integer scores
 *                for specific interest, general interest, and the overall interest that goes into making the graph
 * scaling_udf:   name of a udf that takes a 3-tuple of (total_specific_score, total_general_score, total_graph_score)
 *                and returns a tuple (scaled_specific_score, scaled_general_score, scaled_graph_score).
 *                fields in outputSchema must be float, not double.
 *                ex. use a logistic function to create diminishing
 * -->
 * ui_affinities: {user, item, specific_interest, general_interest, graph_score}
 */
DEFINE ParsedEvents_To_UIAffinities(events, valuation_udf, scaling_udf)
returns ui_affinities {
    events_valued       =   FOREACH $events GENERATE
                                user, item, 
                                FLATTEN($valuation_udf(type)) AS (specific_interest, general_interest, graph_score);

    ui_totals           =   FOREACH (GROUP events_valued BY (user, item)) GENERATE
                                FLATTEN(group) AS (user, item),
                                (float) SUM(events_valued.specific_interest) AS specific_interest,
                                (float) SUM(events_valued.general_interest) AS general_interest,
                                (float) SUM(events_valued.graph_score) AS graph_score;

    $ui_affinities      =   FOREACH ui_totals GENERATE
                                user, item,
                                FLATTEN($scaling_udf(specific_interest, general_interest, graph_score))
                                AS (specific_interest, general_interest, graph_score);
};

/*
 * ui_affinities: {user, item, specific_interest, general_interest, graph_score}
 * -->
 * item_activity_totals: {item, activity}
 */
DEFINE UIAffinities_To_ItemActivityTotals(ui_affinities)
returns item_activities {
    $item_activities    =   FOREACH (GROUP $ui_affinities BY item) GENERATE
                                group AS item,
                                (float) SUM($ui_affinities.graph_score) AS activity;  
};

/*
 * ui_affinities: {user: int, item: int, graph_score: float}
 * item_metadata: {item: int, is_valid_rec: int, activity: float}
 * min_link_weight: float
 * -->
 * ii_links: {from: int, toL int, weight: float, to_activity: float}
 */
DEFINE UIAffinities_To_IISimilarityLinks(ui_affinities, item_metadata, min_link_weight)
returns ii_links {
    ui_copy             =   FOREACH $ui_affinities GENERATE *;
    ui_joined           =   JOIN $ui_affinities BY user, ui_copy BY user;
    ui_filtered         =   FILTER ui_joined BY $ui_affinities::item != ui_copy::item;

    ii_link_terms       =   FOREACH ui_filtered GENERATE
                                $ui_affinities::item AS from,
                                ui_copy::item AS to,
                                ($ui_affinities::graph_score < ui_copy::graph_score ?
                                    $ui_affinities::graph_score : ui_copy::graph_score
                                ) AS weight;
    agg_ii_links        =   FOREACH (GROUP ii_link_terms BY (from, to)) GENERATE
                                FLATTEN(group) AS (from, to),
                                (float) SUM(ii_link_terms.weight) AS weight: float;
    ii_links_tmp        =   FILTER agg_ii_links BY weight > $min_link_weight;

    links_with_metadata =   JOIN ii_links_tmp BY to, $item_metadata BY item;
    ii_filtered         =   FILTER links_with_metadata BY $item_metadata::is_valid_rec == 1;
    $ii_links           =   FOREACH ii_filtered GENERATE
                                from AS from, to AS to, weight AS weight,
                                activity AS to_activity;
};

/*
 * ii_d_mat: {row, col, val: float}
 * -->
 * trimmed: {row, col, val: float}
 */
DEFINE TrimIIDistanceMatrix(ii_d_mat, neighborhood_size)
returns trimmed {
    $trimmed            =   FOREACH (GROUP $ii_d_mat BY row) {
                                closest = ORDER $ii_d_mat BY val ASC;
                                neighborhood = LIMIT closest $neighborhood_size;
                                GENERATE FLATTEN(neighborhood) AS (row, col, val);
                            }
};

/*
 * ii_links: {from, to, weight: float}
 * bayesian_prior: float
 * neighborhood_size: int
 * -->
 * ii_d_mat: {row, col, val: float}
 */
DEFINE IILinksWithActivity_To_IIDistanceMatrix(ii_links, bayesian_prior, neighborhood_size)
returns ii_d_mat {
    distance_mat        =   FOREACH $ii_links GENERATE
                                from AS row, to AS col,
                                (float) ((to_activity + $bayesian_prior) / weight) AS val;
    $ii_d_mat           =   TrimIIDistanceMatrix(distance_mat, $neighborhood_size);
};

/*
 * ii_d_mat: {row, col, val: float}
 * neighborhood_size: int
 * -->
 * ii_d_mat_deep: {row, col, val: float}
 */
DEFINE IIDistanceMatrixShortestPaths(ii_d_mat, neighborhood_size)
returns ii_d_mat_deep {
    adj_mat, vertices   =   AddSelfLoops($ii_d_mat);

    squared             =   MatrixMinPlusSquared(adj_mat);
    trimmed             =   TrimIIDistanceMatrix(squared, $neighborhood_size);

    to_the_fourth       =   MatrixMinPlusSquared(trimmed);
    trimmed_2           =   TrimIIDistanceMatrix(to_the_fourth, $neighborhood_size);

    $ii_d_mat_deep      =   FILTER trimmed_2 BY (row != col);
};

/*
 * d_mat_shallow: {row, col, val: float}
 * d_mat_deep: {row, col, val: float}
 * item_scores: {item_id, score: float} -- all scores must be non-zero (its ok to filter an then lose stuff in join)
 * neighborhood_size: int
 * -->
 * item_neighborhoods: {item, rank, neighbor}
 */
DEFINE IIShallowAndDeepDistanceMatrices_To_ItemNeighborhoods(d_mat_shallow, d_mat_deep, item_scores, neighborhood_size)
returns item_neighborhoods {
    affinities_shallow  =   FOREACH d_mat_shallow GENERATE
                                row, col,
                                1.0f / val AS affinity;

    shallow_enum        =   FOREACH (GROUP affinities_shallow BY row) {
                                sorted = ORDER affinities_shallow BY affinity DESC;
                                GENERATE FLATTEN(EnumerateFromOne(sorted))
                                         AS (row, col, affinity, i);
                            }

    affinities_deep     =   FOREACH d_mat_deep GENERATE
                                row, col,
                                1.0f / val AS affinity;

    deep_enum           =   FOREACH (GROUP affinities_deep BY row) {
                                sorted = ORDER affinities_deep BY affinity DESC;
                                GENERATE FLATTEN(EnumerateFromOne(sorted))
                                         AS (row, col, affinity, i);
                            }

    merged              =   UNION shallow_enum, deep_enum;
    merged_2            =   FOREACH (GROUP merged BY (row, col)) GENERATE
                                FLATTEN(group) AS (row, col),
                                -MIN(merged.i) AS minus_i_going_into_merge: long;

    merged_3            =   FOREACH (JOIN merged_2 BY col, item_scores BY item) GENERATE
                                row AS row, col AS col,
                                (double) minus_i_going_into_merge / SQRT(score) AS minus_affinity: double;
                                -- (double) minus_i_going_into_merge / CBRT(score) AS minus_affinity: double;
                                -- (double) minus_i_going_into_merge / LOG(score) AS minus_affinity: double;

    neighborhoods       =   FOREACH (GROUP merged_3 BY row) {
                                sorted = ORDER merged_3 BY minus_affinity DESC;
                                top    = LIMIT sorted $neighborhood_size;
                                GENERATE FLATTEN(EnumerateFromOne(top.(row, col))) AS (item, neighbor, rank);
                            }

    $item_neighborhoods =   FOREACH neighborhoods GENERATE item, (int) rank AS rank, neighbor;
};

/*
 * item_nhoods: {item: int, rank: int, neighbor: int}
 * item_ids: {id: int, name: chararray}
 * -->
 * i_nhoods_with_names: {item: chararray, rank: int, neighbor: chararray}
 */
DEFINE ItemNeighborhoods_To_ItemNHoodsWithNames(item_nhoods, item_ids)
returns i_nhoods_w_names {
    i_nhoods_tmp        =   FOREACH (JOIN $item_nhoods BY item, $item_ids BY id) GENERATE
                                name AS item, rank AS rank, neighbor AS neighbor;

    $i_nhoods_w_names   =   FOREACH (JOIN i_nhoods_tmp BY neighbor, $item_ids BY id) GENERATE
                                item AS item, rank AS rank, name AS neighbor;
};

/*
 * affinities: {user, item, reason, affinity}
 * -->
 * agg_affinities: {user, item, reason, affinity}
 *
 * Just takes the highest ranked occurence of each item being aggregated (no summation)
 */
DEFINE AggregateUserNeighborAffinities(affinities)
returns agg_affinities {
    -- use cantor pairing function for 33% performance gain vs tuple
    affinities_grpd     =   GROUP $affinities BY ((((long) (user + item) * (long) (user + item + 1)) / 2) + item);
    $agg_affinities     =   FOREACH affinities_grpd GENERATE
                                FLATTEN(TOP(1, 3, $affinities))
                                AS (user, item, reason, affinity);
};

/*
 * ui_affinities: {user, item, specific_interest, general_interest, graph_score}
 * item_nhoods: {item, rank, neighbor}
 * item_scores: {item, score}
 * min_rec_item_score: float, never recommend items with an "imporance score" less than this
 * -->
 * specific_affinities: {user, item, reason, affinity}
 * general_affinities: {user, item, reason, affinity}
 */
DEFINE UserNeighborhoodAffinities(ui_affinities, item_nhoods, item_scores, min_rec_item_score)
returns specific_affinities, general_affinities {
    -- this filtering is super important!
    -- since we gave a specific_interest and general_interest score to every user-item pair
    -- even if one of the two scores is 0,
    -- we need to make sure to properly split the data here by filtering for only >0 entries of each type.
    -- otherwise, we get user-item-in-neighborhood affinities with affinity=0,
    -- which can mess up a whole lot of stuff later down the pipeline

    specific_interests      =   FILTER $ui_affinities BY specific_interest > 0;
    general_interests       =   FILTER $ui_affinities BY general_interest > 0;

    nhoods_with_scores      =   FOREACH (JOIN item_nhoods BY neighbor, item_scores BY item) GENERATE
                                    item_nhoods::item AS item,
                                    rank AS rank, neighbor AS neighbor, score AS score;

    specific_nhoods         =   FOREACH (JOIN specific_interests BY item, nhoods_with_scores BY item) GENERATE
                                    user AS user, neighbor AS item, specific_interests::item AS reason,
                                    specific_interest AS interest, rank AS item_rank, score AS item_score;

    general_nhoods          =   FOREACH (JOIN general_interests BY item, nhoods_with_scores BY item) GENERATE
                                    user AS user, neighbor AS item, general_interests::item AS reason,
                                    general_interest AS interest, rank AS item_rank, score AS item_score;

    s_affins_with_dups      =   FOREACH (FILTER specific_nhoods BY item_score >= $min_rec_item_score) GENERATE
                                    user, item, reason,
                                    (interest * LOG(item_score)) / (item_rank + 1) AS affinity;

    g_affins_with_dups      =   FOREACH (FILTER general_nhoods BY item_score >= $min_rec_item_score) GENERATE
                                    user, item, reason,
                                    (interest * LOG(item_score)) / (item_rank + 1) AS affinity;

    $specific_affinities    =   AggregateUserNeighborAffinities(s_affins_with_dups);
    $general_affinities     =   AggregateUserNeighborAffinities(g_affins_with_dups);
};

/*
 * ui_neighborhood_affinities: {user: int, item: int, reason: int, affinity: float}
 * ui_original_affinities: {user: int, item: int, affinity: float}
 * -->
 * filtered: {user: int, item: int, reason: int, affinity: float}
 */
DEFINE FilterAffinitiesAlreadySeen(ui_neighborhood_affinities, ui_original_affinities)
returns filtered {
    joined              =   JOIN $ui_neighborhood_affinities BY (user, item) LEFT OUTER,
                                 $ui_original_affinities BY (user, item);
    $filtered           =   FOREACH (FILTER joined BY $ui_original_affinities::item IS null) GENERATE
                                $ui_neighborhood_affinities::user AS user,
                                $ui_neighborhood_affinities::item AS item,
                                $ui_neighborhood_affinities::reason AS reason,
                                $ui_neighborhood_affinities::affinity AS affinity;
};

/*
 * ui_affinities: {user: int, item: int, reason: int, affinity: float}
 * max_recs_per_user: int
 * -->
 * recommendations: {user: int, rank: int, reason: int, rec: int}
 */
DEFINE RecommendationsFromUIAffinities(ui_affinities, max_recs_per_user)
returns recommendations {
    recommendations_tmp =   FOREACH (GROUP $ui_affinities BY user) {
                                sorted = ORDER $ui_affinities BY affinity DESC;
                                best = LIMIT sorted $max_recs_per_user;
                                GENERATE group AS user,
                                         FLATTEN(EnumerateFromOne(best.($1, $2)))
                                         AS (rec, reason, rank);
                            }

    $recommendations    =   FOREACH recommendations_tmp GENERATE
                                user, (int) rank AS rank, reason, rec;
};

/*
 * ui_affinities: {user: int, item: int, reason: int, affinity: float}
 * recs: {user: int, rank: int, reason: int, rec: int}
 * -->
 * filtered: {user: int, item: int, reason: int, affinity: float}
 */
DEFINE FilterRecommendationsAlreadySeen(ui_affinities, recs)
returns filtered {
    joined              =   JOIN $ui_affinities BY (user, item) LEFT OUTER, $recs BY (user, rec);
    $filtered           =   FOREACH (FILTER joined BY rec IS null) GENERATE
                                  $ui_affinities::user AS user, $ui_affinities::item AS item,
                                  $ui_affinities::reason AS reason, $ui_affinities::affinity AS affinity;
};

/*
 * recs: {user: int, rank: int, reason: int, rec: int}
 * user_ids: {id: int, name: chararray}
 * item_ids: {id: int, name: chararray}
 * -->
 * recs_with_names: {user: chararray, rank: int, reason: chararray, rec: chararray}
 */
DEFINE ReturnRecsFromIntegerIdsToItemNames(recs, user_ids, item_ids)
returns recs_with_names {
    joined_1            =   FOREACH (JOIN $recs BY user, user_ids BY id) GENERATE
                                name AS user, rank AS rank, reason AS reason, rec AS rec;
    joined_2            =   FOREACH (JOIN joined_1 BY reason, item_ids BY id) GENERATE
                                user AS user, rank AS rank, name AS reason, rec AS rec;
    $recs_with_names    =   FOREACH (JOIN joined_2 BY rec, item_ids BY id) GENERATE
                                user AS user, rank AS rank, reason AS reason, name AS rec;
};
