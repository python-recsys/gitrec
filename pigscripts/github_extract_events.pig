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

SET default_parallel $DEFAULT_PARALLEL;

REGISTER 's3n://mortar-prod-sandbox/jpacker/jar/datafu.jar';
DEFINE EnumerateFromOne datafu.pig.bags.Enumerate('1');

REGISTER '../udfs/jython/github_recommender_udfs.py' USING streaming_python AS udfs;

IMPORT '../macros/recommender.pig';

----------------------------------------------------------------------------------------------------

-- load the raw event logs
events              =   LOAD '$EVENT_LOGS_INPUT_PATH'
                        USING org.apache.pig.piggybank.storage.JsonLoader('
                            actor: chararray,
                            actor_attributes: (
                                gravatar_id: chararray,
                                company: chararray
                            ),
                            repository: (
                                owner: chararray,
                                name: chararray,
                                fork: chararray,
                                language: chararray,
                                description: chararray,
                                watchers: int,
                                stargazers: int,
                                forks: int
                            ),
                            type: chararray,
                            created_at: chararray
                        ');

-- filter to take only events that signal that a user
-- is interested in or a contributor to a repo: Fork, Pull Request, Push, and Watch
events_filtered     =   FILTER events BY (
                            (
                                actor is not null AND
                                repository.owner is not null AND 
                                repository.name is not null AND
                                repository.fork is not null AND
                                type is not null AND
                                created_at is not null
                            )

                            AND

                            (
                                type == 'ForkEvent' OR
                                type == 'PullRequestEvent' OR
                                type == 'PushEvent' OR
                                type == 'WatchEvent'
                            )
                        );

events_renamed      =   FOREACH events_filtered GENERATE
                            actor AS user,
                            CONCAT(repository.owner, CONCAT('/', repository.name)) AS item: chararray,
                            repository AS metadata,
                            type,
                            created_at AS timestamp;

-- not sure why this is necessary given the non-null checks above, but it is.
-- maybe there are event logs with empty-string repo owner fields?
events_2            =   FILTER events_renamed BY SUBSTRING(item, 0, 1) != '/';

----------------------------------------------------------------------------------------------------

-- get the gravatar ids for each user
-- you can get an image from a gravatar id by hitting:
-- http://www.gravatar.com/avatar/[the gravatar id]

events_for_gravatar =   FOREACH (FILTER events_filtered BY SIZE(actor_attributes.gravatar_id) == 32) GENERATE
                            actor AS user,
                            actor_attributes.gravatar_id AS gravatar_id,
                            created_at AS timestamp;

latest_by_user      =   FOREACH (GROUP events_for_gravatar BY user) GENERATE
                            FLATTEN(TOP(1, 2, events_for_gravatar))
                            AS (user, gravatar_id, timestamp);

gravatar_ids        =   FOREACH latest_by_user GENERATE user, gravatar_id;

----------------------------------------------------------------------------------------------------

-- assign an integer id for each user and repo name for better performance
-- we will go back from ids to names when we output final recommendations

user_ids            =   AssignIntegerIds(events_2, 'user');
item_ids            =   AssignIntegerIds(events_2, 'item');

events_id_join_1    =   FOREACH (JOIN events_2 BY user, user_ids BY name) GENERATE
                            user_ids::id AS user,
                            item AS item, metadata AS metadata, type AS type, timestamp AS timestamp;
parsed_events       =   FOREACH (JOIN events_id_join_1 BY item, item_ids BY name) GENERATE
                            user AS user,
                            item_ids::id AS item,
                            metadata AS metadata, type AS type, timestamp AS timestamp;

-- our model does not use content-based filtering or temporal information, so we throw those out
-- (we use parsed_events which has repo metadata at the end to show metadata for each recommendation though)

parsed_events_trim  =   FOREACH parsed_events GENERATE user, item, type;

----------------------------------------------------------------------------------------------------

-- give a weighting to each event and aggregate for each unique (user, item) pair to get an "affinity score"
-- we use a logistic scaling function (every affinity score is between 0 and 1),
-- so if a user pushes many many times to a repo, they won't get a super high affinity score
-- that would mess up later steps in the algorithm.
-- see udfs/jython/github_recommender_udfs.py

ui_affinities       =   ParsedEvents_To_UIAffinities(
                            parsed_events_trim,
                            'udfs.value_event',
                            'udfs.scale_ui_affinity_scores'
                        );

-- aggregate affinity scores for each unique repo

item_activity       =   UIAffinities_To_ItemActivityTotals(ui_affinities);

----------------------------------------------------------------------------------------------------

-- we have repo metadata with every event, but we only want the metadata
-- for the most recent state of the repo

most_recent_events  =   FOREACH (GROUP parsed_events BY item) GENERATE
                            FLATTEN(TOP(1, 4, parsed_events))
                            AS (user, item, metadata, type, created_at);

item_metadata_tmp   =   FOREACH most_recent_events GENERATE
                            item,
                            (metadata.fork == 'true'? 0 : 1) AS is_valid_rec: int,
                            (metadata.language is null ? 'Unknown' : metadata.language) AS language,
                            metadata.forks AS num_forks,
                            metadata.stargazers AS num_stars,
                            metadata.description AS description,
                            2 * metadata.forks + metadata.stargazers AS popularity;

-- the "score" field is a combined measure of popularity and activity
-- this is necessary because repos like django-old have lots of stars,
-- but are abandoned, so they should not be considered as recommendations

item_metadata       =   FOREACH (JOIN item_metadata_tmp BY item, item_activity BY item) GENERATE
                            item_metadata_tmp::item AS item,
                            is_valid_rec AS is_valid_rec,
                            activity AS activity,
                            num_forks AS num_forks,
                            num_stars AS num_stars,
                            (float) SQRT(popularity * activity) AS score,
                            language AS language,
                            description AS description;

----------------------------------------------------------------------------------------------------

rmf $USER_GRAVATAR_IDS_OUTPUT_PATH;
rmf $USER_IDS_OUTPUT_PATH;
rmf $ITEM_IDS_OUTPUT_PATH;
rmf $USER_ITEM_AFFINITIES_OUTPUT_PATH;
rmf $ITEM_METADATA_OUTPUT_PATH;

STORE gravatar_ids  INTO '$USER_GRAVATAR_IDS_OUTPUT_PATH'      USING PigStorage();
STORE user_ids      INTO '$USER_IDS_OUTPUT_PATH'               USING PigStorage();
STORE item_ids      INTO '$ITEM_IDS_OUTPUT_PATH'               USING PigStorage();
STORE ui_affinities INTO '$USER_ITEM_AFFINITIES_OUTPUT_PATH'   USING PigStorage();
STORE item_metadata INTO '$ITEM_METADATA_OUTPUT_PATH'          USING PigStorage();
