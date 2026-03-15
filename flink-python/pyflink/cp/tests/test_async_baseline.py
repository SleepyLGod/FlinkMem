#!/usr/bin/env python
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

"""
Minimal AsyncDataStream smoke test.

Validates that the isolated environment can run a PyFlink DataStream job with
AsyncDataStream.unordered_wait in process/loopback mode end-to-end.

Expected output: 5 records printed to stdout (values 2, 4, 6, 8, 10 in any order).
"""

import asyncio

# Bootstrap: ensure pyflink.cp is importable via symlink
import os, pathlib, pyflink as _pf  # noqa: E401
_cp_src = pathlib.Path(__file__).resolve().parents[1]
_cp_dst = pathlib.Path(_pf.__file__).parent / "cp"
if not _cp_dst.exists():
    os.symlink(_cp_src, _cp_dst)

from pyflink.common import Time, Types
from pyflink.datastream import StreamExecutionEnvironment, AsyncDataStream
from pyflink.datastream.functions import AsyncFunction


class MockAsyncFunction(AsyncFunction):
    """Minimal async function: sleeps briefly, returns sum of two fields."""

    async def async_invoke(self, value):
        await asyncio.sleep(0.1)
        return [value[0] + value[1]]

    def timeout(self, value):
        return [value[0] + value[1]]


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    ds = env.from_collection(
        [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)],
        type_info=Types.ROW_NAMED(["v1", "v2"], [Types.INT(), Types.INT()]),
    )

    result = AsyncDataStream.unordered_wait(
        ds, MockAsyncFunction(), Time.seconds(10), 2, Types.INT()
    )

    result.print()
    env.execute("async_baseline_smoke_test")


if __name__ == "__main__":
    main()

