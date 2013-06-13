#
# Copyright 2013 Mortar Data Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# See README.md for an explanation of each of the 3 steps we use to generate recommendations.
#

from org.apache.pig.scripting import Pig

project_name           = "Github Recommender"
root_path              = # FILL THIS IN
#root_path              = "s3n://mortar-prod-sandbox/jpacker/github"

# All paths below are relative to the root path

# input path
raw_events             = # FILL THIS IN
#raw_events             = "raw_events/*/*/*/*"

# intermediate output paths
user_ids               = "user_ids"
item_ids               = "item_ids"
user_item_affinities   = "user_item_affinities"
item_metadata          = "item_metadata"
item_neighborhoods     = "item_neighborhoods"

# final denormalized outputs (suitable for insertion into DynamoDB or any NoSQL datastore)
item_recs              = "item_recs_log"
user_specific_recs     = "user_specific_recs"
user_general_recs      = "user_general_recs"
user_gravatar_ids      = "user_gravatar_ids"

# set this to ([# nodes you plan to use in your cluster] - 1) * 3
default_parallel       = # FILL THIS IN
#default_parallel       = 30

class ControlscriptStep:
    def __init__(self, action, script, params):
        self.action = action
        self.script = "../pigscripts/" + script + ".pig"
        self.params = { "DEFAULT_PARALLEL": default_parallel }
        for k, v in params.iteritems():
            if k.endswith("INPUT_PATH") or k.endswith("OUTPUT_PATH"):
                self.params[k] = root_path + "/" + v
            else:
                self.params[k] = v

    def run(self):
        print project_name + ": " + self.action
        compiled = Pig.compileFromFile(self.script)
        bound = compiled.bind(self.params)
        return bound.runSingle()

steps = {
    "extract_events" : ControlscriptStep(
        "extracting user-item interactions and item metadata from raw event logs",
         "github_extract_events", {
            "EVENT_LOGS_INPUT_PATH"              : raw_events,
            "USER_IDS_OUTPUT_PATH"               : user_ids,
            "ITEM_IDS_OUTPUT_PATH"               : item_ids,
            "USER_GRAVATAR_IDS_OUTPUT_PATH"      : user_gravatar_ids,
            "USER_ITEM_AFFINITIES_OUTPUT_PATH"   : user_item_affinities,
            "ITEM_METADATA_OUTPUT_PATH"          : item_metadata
         }),
    "gen_item_recs" : ControlscriptStep(
        "generating recommendations for each item",
        "github_gen_item_recs", {
            "ITEM_IDS_INPUT_PATH"                : item_ids,
            "USER_ITEM_AFFINITIES_INPUT_PATH"    : user_item_affinities,
            "ITEM_METADATA_INPUT_PATH"           : item_metadata,
            "ITEM_NEIGHBORHOODS_OUTPUT_PATH"     : item_neighborhoods,
            "ITEM_RECS_OUTPUT_PATH"              : item_recs
        }),
    "gen_user_recs" : ControlscriptStep(
        "generating recommendations for each user",
        "github_gen_user_recs", {
            "USER_IDS_INPUT_PATH"                : user_ids,
            "ITEM_IDS_INPUT_PATH"                : item_ids,
            "USER_ITEM_AFFINITIES_INPUT_PATH"    : user_item_affinities,
            "ITEM_NEIGHBORHOODS_INPUT_PATH"      : item_neighborhoods,
            "ITEM_METADATA_INPUT_PATH"           : item_metadata,
            "USER_SPECIFIC_RECS_OUTPUT_PATH"     : user_specific_recs,
            "USER_GENERAL_RECS_OUTPUT_PATH"      : user_general_recs
        })
}

if __name__ == "__main__":
    steps["extract_events"].run()
    steps["gen_item_recs"].run()
    steps["gen_user_recs"].run()
