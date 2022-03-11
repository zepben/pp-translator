#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

__all__ = ["get_upstream_end_to_tns"]

from typing import List, Tuple, TypeVar

from zepben.evolve import PowerTransformerEnd, FeederDirection

T = TypeVar("T")


def get_upstream_end_to_tns(
        ends_to_topological_nodes: List[Tuple[PowerTransformerEnd, T]]
) -> List[Tuple[PowerTransformerEnd, T]]:
    return [(end, tn) for (end, tn) in ends_to_topological_nodes
            if tn is not None
            and end is not None
            and end.terminal.normal_feeder_direction == FeederDirection.UPSTREAM]
