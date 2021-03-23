#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

from typing import FrozenSet, Tuple, Iterable, List

import pandapower as pp
from zepben.evolve import Terminal, NetworkService, AcLineSegment, PerLengthSequenceImpedance, WireInfo, \
    PowerTransformer, EnergySource, EnergyConsumer, ConductingEquipment, Location


def create_pp_bus(
        bus_branch_model: pp.pandapowerNet,
        base_voltage: int,
        negligible_impedance_equipment: FrozenSet[ConductingEquipment],
        border_terminals: FrozenSet[Terminal],
        inner_terminals: FrozenSet[Terminal],
        node_breaker_model: NetworkService
) -> int:
    locations: List[Location] = [t.conducting_equipment.location for t in border_terminals
                                 if t.conducting_equipment.location is not None]

    coords = [(p.x_position, p.y_position) for location in locations for p in location.points]

    return pp.create_bus(
        bus_branch_model,
        vn_kv=base_voltage / 1000,
        name=f"bus_{_create_id_from_terminals(border_terminals)}",
        geodata=coords[0]
    )


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
    locations: List[Location] = [acls.location for acls in common_lines]
    coords = [(p.x_position, p.y_position) for location in locations for p in location.points]
    length = length / 1000

    pp.create_line(
        bus_branch_model,
        from_bus=line_busses[0],
        to_bus=line_busses[1],
        length_km=0.001 if length == 0 else length,
        name=f"line_{_create_id_from_terminals(border_terminals)}",
        std_type=line_type,
        geodata=coords
    )


def get_line_type_id(
        per_length_sequence_impedance: PerLengthSequenceImpedance,
        wire_info: WireInfo,
        voltage: int
) -> str:
    # TODO: This needs to be implemented properly to generate a unique key for each line type
    if voltage < 1000:
        return "NAYY 4x50 SE"
    elif voltage < 12000:
        return "NA2XS2Y 1x95 RM/25 12/20 kV"
    else:
        return "N2XS(FL)2Y 1x120 RM/35 64/110 kV"


def create_pp_line_type(bus_branch_model: pp.pandapowerNet, per_length_sequence_impedance: PerLengthSequenceImpedance,
                        wire_info: WireInfo, voltage: int) -> str:
    # TODO: This needs to be implemented properly to create an std_type for the line
    return get_line_type_id(per_length_sequence_impedance, wire_info, voltage)


def create_pp_transformer(bus_branch_model: pp.pandapowerNet, pt: PowerTransformer, busses: Tuple[int, int],
                          pt_type: str, node_breaker_model: NetworkService):
    pp.create_transformer(bus_branch_model, hv_bus=busses[0], lv_bus=busses[1], std_type=pt_type, name=pt.name)


def get_transformer_type_id(pt: PowerTransformer) -> str:
    # TODO: This needs to be implemented properly to generate a unique key for each transformer type
    return "0.25 MVA 10/0.4 kV"


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
