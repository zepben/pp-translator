__all__ = ["get_upstream_topological_nodes"]

from typing import List, Tuple, TypeVar

from zepben.evolve import PowerTransformerEnd, SinglePhaseKind, PhaseDirection

T = TypeVar("T")


def get_upstream_topological_nodes(ends_to_topological_nodes: List[Tuple[PowerTransformerEnd, T]]) -> List[T]:
    return [tn for (end, tn) in ends_to_topological_nodes
            if tn is not None
            and end is not None
            # TODO: Check how to account for the fact that this can have phases other than A and they can have different directions. For now given three-phase system this works
            and end.terminal.traced_phases.direction_normal(SinglePhaseKind.A).has(PhaseDirection.IN)]
