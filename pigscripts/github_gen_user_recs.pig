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

%default NUM_SPECIFIC_RECS 20
%default NUM_GENERAL_RECS 20
%default MIN_REC_ITEM_SCORE 2.71828 -- items for which the "importance score" field in item_metadata is less than this
                                    -- will not be considered as recommendations. what to set this at depends
                                    -- on how the scores are calculated, but this is a decent setting for our metric.
SET default_parallel $DEFAULT_PARALLEL;

REGISTER 's3n://mortar-prod-sandbox/jpacker/jar/datafu.jar';
DEFINE EnumerateFromOne datafu.pig.bags.Enumerate('1');

IMPORT '../macros/recommender.pig';

----------------------------------------------------------------------------------------------------

user_ids                =   LOAD '$USER_IDS_INPUT_PATH' USING PigStorage()
                            AS (id: int, name: chararray);

item_ids                =   LOAD '$ITEM_IDS_INPUT_PATH' USING PigStorage()
                            AS (id: int, name: chararray);

ui_affinities           =   LOAD '$USER_ITEM_AFFINITIES_INPUT_PATH' USING PigStorage()
                            AS (user: int, item: int,
                                specific_interest: float, general_interest: float, graph_score: float);

item_nhoods             =   LOAD '$ITEM_NEIGHBORHOODS_INPUT_PATH' USING PigStorage()
                            AS (item: int, rank: int, neighbor: int);

item_metadata           =   LOAD '$ITEM_METADATA_INPUT_PATH' USING PigStorage()
                            AS (item: int, is_valid_rec: int,
                                activity: float, num_forks: int, num_stars: int, score: float,
                                language: bytearray, description: bytearray);

item_metadata_2         =   FOREACH (JOIN item_metadata BY item, item_ids BY id) GENERATE
                                item_ids::name AS item,
                                language AS language,
                                num_forks AS num_forks,
                                num_stars AS num_stars,
                                description AS description;

-- never recommend repos without at least a few forks/stars
item_scores             =   FOREACH item_metadata GENERATE item, score;

----------------------------------------------------------------------------------------------------

-- given an affinity between user U and item I, calulate affinities between U and each item in the neighborhood of I
-- we calculate two types of affinities: "specific", which measures how similar a repo is to those you've contributed to,
-- and "general" which measures how similar a repo is to those you've watched/forked
--
-- we then generate recommendations by first filtering out obviously bad recommendations,
-- and then taking the top N user-item pairs by affinity

specific_affinities, general_affinities =   UserNeighborhoodAffinities(ui_affinities,
                                                                       item_nhoods,
                                                                       item_scores,
                                                                       $MIN_REC_ITEM_SCORE);

specific_affinities_filt        =   FilterAffinitiesAlreadySeen(specific_affinities, ui_affinities);
specific_recs_tmp               =   RecommendationsFromUIAffinities(specific_affinities_filt, $NUM_SPECIFIC_RECS);

-- the extra filter is so that we don't show recommendations in general that are already in specific
general_affinities_filt_tmp     =   FilterAffinitiesAlreadySeen(general_affinities, ui_affinities);
general_affinities_filt         =   FilterRecommendationsAlreadySeen(general_affinities_filt_tmp, specific_recs_tmp);
general_recs_tmp                =   RecommendationsFromUIAffinities(general_affinities_filt, $NUM_GENERAL_RECS);

----------------------------------------------------------------------------------------------------

-- postprocesses the recommendations by filtering out some more dumb ones

DEFINE PostprocessRecs(recs, user_ids, item_ids, item_metadata)
returns new_recs {
    recs_with_names     =   ReturnRecsFromIntegerIdsToItemNames($recs, $user_ids, $item_ids);

    -- don't recommend repos that the user owns
    filtered            =   FILTER recs_with_names BY user != REGEX_EXTRACT(rec, '(.*)/(.*)', 1);
    reranked_1          =   FOREACH (GROUP filtered BY user) {
                                sorted = ORDER filtered BY rank ASC;
                                GENERATE FLATTEN(EnumerateFromOne(sorted))
                                         AS (user, old_rank, reason, rec, new_rank);
                            }
    reranked_2          =   FOREACH reranked_1 GENERATE user, (int) new_rank AS rank: int, reason, rec;

    new_recs_tmp        =   FOREACH (JOIN reranked_2 BY rec, $item_metadata BY item) GENERATE
                                user AS user, rank AS rank, reason AS reason, rec AS rec,
                                language AS language,
                                num_forks AS num_forks,
                                num_stars AS num_stars,
                                description AS description;

    $new_recs           =   FOREACH (GROUP new_recs_tmp BY user) {
                                sorted = ORDER new_recs_tmp BY rank ASC;
                                GENERATE FLATTEN(sorted);
                            }
}

specific_recs           =   PostprocessRecs(specific_recs_tmp, user_ids, item_ids, item_metadata_2);
general_recs            =   PostprocessRecs(general_recs_tmp, user_ids, item_ids, item_metadata_2);

-- and we're done. yay!

----------------------------------------------------------------------------------------------------

rmf $USER_SPECIFIC_RECS_OUTPUT_PATH;
rmf $USER_GENERAL_RECS_OUTPUT_PATH;

STORE specific_recs  INTO '$USER_SPECIFIC_RECS_OUTPUT_PATH' USING PigStorage();
STORE general_recs   INTO '$USER_GENERAL_RECS_OUTPUT_PATH'  USING PigStorage();