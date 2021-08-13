__all__ = ["get_upstream_end_to_tns"]

from typing import List, Tuple, TypeVar

from zepben.evolve import PowerTransformerEnd, SinglePhaseKind, PhaseDirection

T = TypeVar("T")


def get_upstream_end_to_tns(
        ends_to_topological_nodes: List[Tuple[PowerTransformerEnd, T]]
) -> List[Tuple[PowerTransformerEnd, T]]:
    return [(end, tn) for (end, tn) in ends_to_topological_nodes
            if tn is not None
            and end is not None
            # TODO: How to account for the fact you can have phases with different directions??
            and (end.terminal.traced_phases.direction_normal(SinglePhaseKind.A).has(PhaseDirection.IN)
                 or end.terminal.traced_phases.direction_normal(SinglePhaseKind.B).has(PhaseDirection.IN)
                 or end.terminal.traced_phases.direction_normal(SinglePhaseKind.C).has(PhaseDirection.IN))
            ]
