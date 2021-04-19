#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

from typing import FrozenSet, Tuple, List, Callable, Iterable, TypeVar

import pandapower as pp
from zepben.evolve import Terminal, NetworkService, AcLineSegment, PerLengthSequenceImpedance, WireInfo, \
    PowerTransformer, EnergySource, ConductingEquipment, Location

__all__ = [
    "create_pp_bus",
    "create_pp_line",
    "get_line_type_id",
    "create_pp_line_type",
    "create_pp_transformer",
    "get_transformer_type_id",
    "create_pp_transformer_type",
    "create_pp_grid_connection",
    "create_pp_load",
    "create_transformer_or_hv_load"
]


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
        vn_kv=11 if base_voltage == 12700 else (base_voltage / 1000),
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
) -> int:
    locations: List[Location] = [acls.location for acls in common_lines]
    coords = [(p.x_position, p.y_position) for location in locations for p in location.points]
    voltage = [l.base_voltage.nominal_voltage for l in common_lines][0]
    length = (length * 3 if voltage == 12700 else length) / 1000

    return pp.create_line(
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
    # TODO: This needs to be implemented properly to generate a unique key for each line type instead of \
    #  using hard-coded line types based on voltage
    if voltage < 1000:
        return "NAYY 4x50 SE"
    elif voltage < 12000:
        return "NA2XS2Y 1x95 RM/25 12/20 kV"
    else:
        return "SWER"


def create_pp_line_type(bus_branch_model: pp.pandapowerNet, per_length_sequence_impedance: PerLengthSequenceImpedance,
                        wire_info: WireInfo, voltage: int) -> str:
    # TODO: This needs to be implemented properly to create an std_type for the line
    line_type_id = get_line_type_id(per_length_sequence_impedance, wire_info, voltage)

    if line_type_id == "SWER":
        pp.create_std_type(
            bus_branch_model,
            {
                "c_nf_per_km": 6.7,
                "r_ohm_per_km": 10.0,
                "x_ohm_per_km": 2.5,
                "max_i_ka": 0.252,
                "type": "cs",
                "q_mm2": 10,
                "alpha": 3.93e-3
            },
            "SWER",
            "line"
        )
        return line_type_id
    else:
        return line_type_id


def create_transformer_or_hv_load(load_provider: Callable[[PowerTransformer], Tuple[int, int]]) -> \
        Callable[[pp.pandapowerNet, PowerTransformer, Tuple[int, int], str, NetworkService], int]:
    def wrapper(bus_branch_model: pp.pandapowerNet, pt: PowerTransformer, busses: Tuple[int, int],
                pt_type: str, node_breaker_model: NetworkService):
        if busses[1] is None:
            return create_pp_load(load_provider)(bus_branch_model, pt, busses[0], node_breaker_model)
        else:
            return create_pp_transformer(bus_branch_model, pt, busses, pt_type, node_breaker_model)

    return wrapper


def create_pp_transformer(bus_branch_model: pp.pandapowerNet, pt: PowerTransformer, busses: Tuple[int, int],
                          pt_type: str, node_breaker_model: NetworkService) -> int:
    if "CPM3B3" in {f.mrid for f in pt.normal_feeders}:
        return pp.create_transformer(
            bus_branch_model,
            hv_bus=busses[0],
            lv_bus=busses[1],
            std_type=pt_type,
            name=pt.name
        )
    else:
        return pp.create_transformer_from_parameters(
            bus_branch_model,
            hv_bus=busses[0],
            lv_bus=busses[1],
            sn_mva=1.0,
            # vn_hv_kv=list(pt.ends)[0].rated_u / 1000,
            # vn_lv_kv=list(pt.ends)[1].rated_u / 1000,
            vn_hv_kv=11,
            vn_lv_kv=11,
            vk_percent=0.01,
            vkr_percent=0.005,
            pfe_kw=0,
            i0_percent=0,
            vector_group="D0",
            name=pt.name
        )


def get_transformer_type_id(pt: PowerTransformer) -> str:
    # TODO: This needs to be implemented properly to generate a unique key for each transformer type
    return "0.25 MVA 11/0.415 kV"


def create_pp_transformer_type(bus_branch_model: pp.pandapowerNet, pt: PowerTransformer) -> str:
    # TODO: This needs to be implemented properly to create an std_type for the transformer \
    #  instead of it being a hardcoded one
    pp.create_std_type(
        bus_branch_model,
        data=
        {
            "sn_mva": 0.25,
            "vn_hv_kv": 11,
            "vn_lv_kv": 0.415,
            "vk_percent": 4,
            "vkr_percent": 1.2,
            "pfe_kw": 0.6,
            "i0_percent": 0.24,
            "shift_degree": 150,
            "vector_group": "Dyn5",
            "tap_side": "hv",
            "tap_neutral": 0,
            "tap_min": -2,
            "tap_max": 2,
            "tap_step_degree": 0,
            "tap_step_percent": 2.5,
            "tap_phase_shifter": False
        },
        name="0.25 MVA 11/0.415 kV",
        element="trafo"
    )
    return "0.25 MVA 11/0.415 kV"


def create_pp_grid_connection(bus_branch_model: pp.pandapowerNet, es: EnergySource, bus: int,
                              node_breaker_model: NetworkService):
    pp.create_ext_grid(bus_branch_model, bus=bus, vm_pu=1, name=es.name)


CE = TypeVar('CE', bound=ConductingEquipment)


def create_pp_load(load_provider: Callable[[CE], Tuple[int, int]]) -> \
        Callable[[pp.pandapowerNet, CE, int, NetworkService], int]:
    def creator(bus_branch_model: pp.pandapowerNet,
                ce: CE,
                bus: int,
                node_breaker_model: NetworkService) -> int:
        p, q = load_provider(ce)
        return pp.create_load(bus_branch_model, bus=bus, p_mw=p / 1000000, q_mvar=q / 1000000, name=ce.name)

    return creator


def _create_id_from_terminals(ts: Iterable[Terminal]):
    "_".join(sorted((t.mrid for t in ts)))
    pass
