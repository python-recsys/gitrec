"""
Copyright 2013 Mortar Data Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from math import exp
from pig_util import outputSchema

@outputSchema("valuation: (specific_interest: float, general_interest: float, graph_score: float)")
def value_event(event_type):
    if event_type == "PushEvent":
        return (1.00, 0.00, 1.00)
    elif event_type == "ForkEvent":
        return (0.00, 1.00, 0.50)
    elif event_type == "PullRequestEvent":
        return (1.00, 0.00, 1.00)
    elif event_type == "WatchEvent":
        return (0.00, 0.50, 0.25)
    else:
        raise Exception("Recieved invalid event type: " + str(event_type))

@outputSchema("scaled_scores: (specific_interest: float, general_interest: float, graph_score: float)")
def scale_ui_affinity_scores(specific_interest, general_interest, graph_score):
    return (logistic_scale(specific_interest),
            logistic_scale(general_interest),
            logistic_scale(graph_score))

LOGISTIC_PARAM = 3.0 / 18.0
def logistic_scale(value):
    return 2.0 / (1.0 + exp(-LOGISTIC_PARAM * value)) - 1.0
