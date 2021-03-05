from typing import FrozenSet, Tuple, Iterable

import pandapower as pp
from zepben.evolve import Terminal, NetworkService, AcLineSegment, PerLengthSequenceImpedance, WireInfo, \
    PowerTransformer, EnergySource, EnergyConsumer, ConductingEquipment


def create_pp_bus(
        bus_branch_model: pp.pandapowerNet,
        base_voltage: int,
        negligible_impedance_equipment: FrozenSet[ConductingEquipment],
        border_terminals: FrozenSet[Terminal],
        inner_terminals: FrozenSet[Terminal],
        node_breaker_model: NetworkService
) -> int:
    return pp.create_bus(bus_branch_model, vn_kv=base_voltage / 1000,
                         name=f"bus_{_create_id_from_terminals(border_terminals)}")


def create_pp_line(
        bus_branch_model: pp.pandapowerNet,
        line_busses: Tuple[int, int],
        length: float,
        line_type: str,
        common_lines: FrozenSet[AcLineSegment],
        border_terminals: FrozenSet[Terminal],
        inner_terminals: FrozenSet[Terminal],
        node_breaker_model: NetworkService
):
    pp.create_line(
        bus_branch_model,
        from_bus=line_busses[0],
        to_bus=line_busses[1],
        length_km=length / 1000,
        name=f"line_{_create_id_from_terminals(border_terminals)}",
        std_type=line_type
    )


def get_line_type_id(per_length_sequence_impedance: PerLengthSequenceImpedance, wire_info: WireInfo) -> str:
    # TODO: This needs to be implemented properly to generate a unique key for each line type
    return "NAYY 4x50 SE"


def create_pp_line_type(bus_branch_model: pp.pandapowerNet, per_length_sequence_impedance: PerLengthSequenceImpedance,
                        wire_info: WireInfo) -> str:
    # TODO: This needs to be implemented properly to create an std_type for the line
    return get_line_type_id(per_length_sequence_impedance, wire_info)


def create_pp_transformer(bus_branch_model: pp.pandapowerNet, pt: PowerTransformer, busses: Tuple[int, int],
                          pt_type: str, node_breaker_model: NetworkService):
    pp.create_transformer(bus_branch_model, hv_bus=busses[0], lv_bus=busses[1], std_type=pt_type, name=pt.name)


def get_transformer_type_id(pt: PowerTransformer) -> str:
    # TODO: This needs to be implemented properly to generate a unique key for each transformer type
    return "0.4 MVA 20/0.4 kV"


def create_pp_transformer_type(bus_branch_model: pp.pandapowerNet, pt: PowerTransformer) -> str:
    # TODO: This needs to be implemented properly to create an std_type for the transformer
    return get_transformer_type_id(pt)


def create_pp_grid_connection(bus_branch_model: pp.pandapowerNet, es: EnergySource, bus: int,
                              node_breaker_model: NetworkService):
    pp.create_ext_grid(bus_branch_model, bus=bus, vm_pu=1.02, name=es.name)


def create_pp_load(
        bus_branch_model: pp.pandapowerNet,
        ec: EnergyConsumer,
        bus: int,
        node_breaker_model: NetworkService
):
    pp.create_load(bus_branch_model, bus=bus, p_mw=ec.p / 1000000, q_mvar=ec.q / 1000000, name=ec.name)


def _create_id_from_terminals(ts: Iterable[Terminal]):
    "_".join(sorted((t.mrid for t in ts)))
    pass
