# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""V0.2 stateful semantic operators — keyed process functions, state machines, and timers."""

from pyflink.semantic_runtime.stateful.semantic_window import (  # noqa: F401
    SemWindowFunction, SemWindowConfig,
)
from pyflink.semantic_runtime.stateful.sem_groupby_stateful import (  # noqa: F401
    SemGroupbyFunction, SemGroupbyConfig,
)
from pyflink.semantic_runtime.stateful.sem_groupby_window import (  # noqa: F401
    WindowOwnedSemGroupbyFunction,
)
from pyflink.semantic_runtime.stateful.sem_agg_stateful import (  # noqa: F401
    SemAggFunction, SemAggConfig,
)
from pyflink.semantic_runtime.stateful.sem_agg_window import (  # noqa: F401
    WindowOwnedSemAggFunction,
)
from pyflink.semantic_runtime.stateful.sem_agg_pipeline import (  # noqa: F401
    build_sem_agg_operator,
    resolve_agg_execution_plan,
    AggExecutionPlan,
)
from pyflink.semantic_runtime.stateful.cts_retrieve import (  # noqa: F401
    CtsRetrieveFunction, CtsRetrieveConfig,
    # V0.2+ public aliases
    SemSearchFunction, SemSearchConfig,
)
from pyflink.semantic_runtime.stateful.sem_topk_continuous import (  # noqa: F401
    SemTopKFunction, SemTopKConfig,
)
from pyflink.semantic_runtime.stateful.sem_topk_pipeline import (  # noqa: F401
    build_sem_topk_pipeline,
)
from pyflink.semantic_runtime.stateful.sem_groupby_pipeline import (  # noqa: F401
    build_sem_groupby_operator,
    resolve_groupby_execution_plan,
    GroupbyExecutionPlan,
)
from pyflink.semantic_runtime.stateful.external_search_backend import (  # noqa: F401
    ExternalSearchBackend, SearchResult, MockSearchBackend, FaissSearchBackend,
    SearchBackendAsyncFn,
)
from pyflink.semantic_runtime.stateful.simple_text_encoder import (  # noqa: F401
    HashingTextEncoder,
)
from pyflink.semantic_runtime.stateful.continuous_rag_workflow import (  # noqa: F401
    ContinuousRAGConfig,
    build_continuous_rag_workflow,
    build_continuous_rag_workflow_from_runtime_config,
)
