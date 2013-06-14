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

%default MIN_LINK_WEIGHT 0.12
%default BAYESIAN_PRIOR 25.0
%default NEIGHBORHOOD_SIZE 20
SET default_parallel $DEFAULT_PARALLEL;

IMPORT '../macros/recommender.pig';

----------------------------------------------------------------------------------------------------

-- load data and project out the fields that we need

item_ids            =   LOAD '$ITEM_IDS_INPUT_PATH' USING PigStorage()
                        AS (id: int, name: chararray);

ui_affinities_tmp   =   LOAD '$USER_ITEM_AFFINITIES_INPUT_PATH' USING PigStorage()
                            AS (user: int, item: int,
                                specific_interest: float, general_interest: float, graph_score: float);
ui_affinities_proj  =   FOREACH ui_affinities_tmp GENERATE user, item, graph_score;

item_metadata_tmp   =   LOAD '$ITEM_METADATA_INPUT_PATH' USING PigStorage()
                        AS (item: int, is_valid_rec: int,
                            activity: float, num_forks: int, num_stars: int, score: float,
                            language: bytearray, description: bytearray);

item_scores_tmp     =   FOREACH item_metadata_tmp GENERATE item, score;
item_scores         =   FILTER item_scores_tmp BY score > 0;

-- used to generate a graph of repo-repo similarities
item_data_for_graph =   FOREACH item_metadata_tmp GENERATE item, is_valid_rec, activity;

-- used to denormalize metadata into the final "repos similar to the given repo" output
item_data_for_recs  =   FOREACH (JOIN item_metadata_tmp BY item, item_ids BY id) GENERATE
                            item_ids::name AS item,
                            language AS language,
                            num_forks AS num_forks,
                            num_stars AS num_stars,
                            description AS description;

----------------------------------------------------------------------------------------------------

-- reduce the graph of user-item affinities to a graph of item-item affinities.
--
-- we use bayes theorem to estimate the probability of a user
-- interacting with item B given that they interacted with item A
-- and call that the affinity between A and B.
--
-- we then say that "distance" is the reciprocal of "affinity",
-- so similar items are "close" and dissimilar items are "far".

ii_links                =   UIAffinities_To_IISimilarityLinks(
                                ui_affinities_proj, item_data_for_graph, $MIN_LINK_WEIGHT
                            );

d_mat_shallow           =   IILinksWithActivity_To_IIDistanceMatrix(
                                ii_links, $BAYESIAN_PRIOR, $NEIGHBORHOOD_SIZE
                            );

-- follow shortest paths on the item-item distance matrix to maximum path-length 4

d_mat_deep              =   IIDistanceMatrixShortestPaths(
                                d_mat_shallow, $NEIGHBORHOOD_SIZE
                            );

-- generate "neighborhoods" for each item based on the shortest paths,
-- except that we blend the "shallow" (1-step only) and "deep" (max of 4 steps) neighborhood rankings
-- to account for both the literal distance and the number of degress of connection taken to get there.
-- kind of confusing, but it was necessary to avoid getting "sucked in" to clusters of super-popular repos
-- like twitter/bootstrap or documentcloud/backbone. the graph generation itself actually got improved
-- after this was implemented, so it might not actually be necessary to blend in d_mat_shallow anymore.

item_nhoods             =   IIShallowAndDeepDistanceMatrices_To_ItemNeighborhoods(
                                d_mat_shallow, d_mat_deep, item_scores, $NEIGHBORHOOD_SIZE
                            );

----------------------------------------------------------------------------------------------------

-- the item neighborhoods are the output sent to the next script, github_gen_user_recs.pig
-- but we also output a version with names and metadata that serves as a
-- "repos similar to the given repo" recommender

item_nhoods_with_names  =   ItemNeighborhoods_To_ItemNHoodsWithNames(item_nhoods, item_ids);

item_recs_tmp           =   FOREACH (JOIN item_nhoods_with_names BY neighbor, item_data_for_recs BY item) GENERATE
                                $0          AS item,
                                rank        AS rank,
                                neighbor    AS rec,
                                language    AS language,
                                num_forks   AS num_forks,
                                num_stars   AS num_stars,
                                description AS description;


item_recs               =   FOREACH (GROUP item_recs_tmp BY item) {
                                sorted = ORDER item_recs_tmp BY rank ASC;
                                GENERATE FLATTEN(sorted);
                            }

----------------------------------------------------------------------------------------------------

rmf $ITEM_NEIGHBORHOODS_OUTPUT_PATH;
rmf $ITEM_RECS_OUTPUT_PATH;

STORE item_nhoods INTO '$ITEM_NEIGHBORHOODS_OUTPUT_PATH' USING PigStorage();
STORE item_recs   INTO '$ITEM_RECS_OUTPUT_PATH'          USING PigStorage();
