#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import logging
from typing import FrozenSet, Tuple, List, Optional, Callable, Dict, Iterable

import pandapower as pp
from zepben.evolve import Terminal, NetworkService, AcLineSegment, PowerTransformer, EnergyConsumer, \
    PowerTransformerEnd, ConductingEquipment, \
    PowerElectronicsConnection, Location, BusBranchNetworkCreator, EnergySource, Switch, Junction, BusbarSection, \
    SinglePhaseKind, PhaseCode, TracedPhases

from pp_creators.utils import get_upstream_end_to_tns
from pp_creators.validators.validator import PandaPowerNetworkValidator

__all__ = ["PandaPowerNetworkCreatorEE", "PpElement"]

P_OPTIONS = {
    SinglePhaseKind.A: "p_a_mw",
    SinglePhaseKind.B: "p_b_mw",
    SinglePhaseKind.C: "p_c_mw"
}

Q_OPTIONS = {
    SinglePhaseKind.A: "q_a_mvar",
    SinglePhaseKind.B: "q_b_mvar",
    SinglePhaseKind.C: "q_c_mvar"
}


class PpElement:
    def __init__(self, index: int, type: str):
        self.index = index
        self.type = type


class PandaPowerNetworkCreatorEE(
    BusBranchNetworkCreator[pp.pandapowerNet, PpElement, PpElement, PpElement, PpElement, PpElement, PpElement,
                            PandaPowerNetworkValidator]
):

    def __init__(
            self, *,
            logger: logging.Logger,
            asymmetric_load: bool = False,
            vm_pu: float = 1.0,
            load_provider: Callable[[ConductingEquipment], Tuple[float, float]] = lambda x: (0, 0),
            pec_load_provider: Callable[[ConductingEquipment], Tuple[float, float]] = lambda x: (0, 0)
    ):
        self.vm_pu = vm_pu
        self.logger = logger
        self.asymmetric_load = asymmetric_load
        self.load_provider = load_provider
        self.pec_load_provider = pec_load_provider

    def bus_branch_network_creator(self, node_breaker_network: NetworkService) -> pp.pandapowerNet:
        return pp.create_empty_network()

    def topological_node_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            base_voltage: Optional[int],
            collapsed_conducting_equipment: FrozenSet[ConductingEquipment],
            border_terminals: FrozenSet[Terminal],
            inner_terminals: FrozenSet[Terminal],
            node_breaker_network: NetworkService
    ) -> Tuple[str, PpElement]:
        locations: List[Location] = [t.conducting_equipment.location for t in border_terminals
                                     if t.conducting_equipment.location is not None]
        coords = [(p.x_position, p.y_position) for location in locations for p in location.points]

        vn_v = base_voltage
        bus_idx = pp.create_bus(
            bus_branch_network,
            vn_kv=vn_v / 1000,
            name=f"bus_{_create_id_from_terminals(border_terminals)}",
            geodata=coords[0]
        )
        return f"bus:{bus_idx}", PpElement(bus_idx, "bus")

    def topological_branch_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            connected_topological_nodes: Tuple[PpElement, PpElement],
            length: Optional[float],
            collapsed_ac_line_segments: FrozenSet[AcLineSegment],
            border_terminals: FrozenSet[Terminal],
            inner_terminals: FrozenSet[Terminal],
            node_breaker_network: NetworkService
    ) -> Tuple[str, PpElement]:
        locations: List[Location] = [acls.location for acls in collapsed_ac_line_segments]
        coords = [(p.x_position, p.y_position) for location in locations for p in location.points]
        voltage = [l.base_voltage.nominal_voltage for l in collapsed_ac_line_segments][0]
        length = (length * 3 if voltage == 12700 else length) / 1000
        line = next(iter(collapsed_ac_line_segments))

        # TODO: The source data has 0 rating lines so we need to add a hack here to make them rated for 1 amp.
        #  Otherwise the pandapower load flow will fail to run due to a division by 0
        rating_ka = (1 if line.wire_info.rated_current == 0 else line.wire_info.rated_current) / 1000

        line_idx = pp.create_line_from_parameters(
            bus_branch_network,
            name=",".join((cacls.name for cacls in collapsed_ac_line_segments)),
            from_bus=connected_topological_nodes[0].index,
            to_bus=connected_topological_nodes[1].index,
            length_km=length,
            r_ohm_per_km=line.per_length_sequence_impedance.r * 1000,
            x_ohm_per_km=line.per_length_sequence_impedance.x * 1000,
            max_i_ka=rating_ka,
            c_nf_per_km=0,
            geodata=coords
        )
        return f"line:{line_idx}", PpElement(line_idx, "line")

    def power_transformer_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            power_transformer: PowerTransformer,
            ends_to_topological_nodes: List[Tuple[PowerTransformerEnd, Optional[PpElement]]],
            node_breaker_network: NetworkService
    ) -> Dict[str, PpElement]:
        mapped_elements: Dict[str, PpElement] = {}

        upstream_end, upstream_tn = get_upstream_end_to_tns(ends_to_topological_nodes)[0]
        downstream_end, downstream_tn = [(end, tn) for (end, tn) in ends_to_topological_nodes if tn != upstream_tn][0]

        upstream_voltage = upstream_end.rated_u
        downstream_voltage = downstream_end.rated_u

        if downstream_tn is None:
            downstream_tn = self._create_downstream_lv_network_and_bus(
                bus_branch_network,
                power_transformer,
                downstream_voltage,
                mapped_elements
            )

        end = next(iter(power_transformer.ends))
        sn_mva = (1000000 if end.rated_s == 0 else end.rated_s) / 1000000
        vn_hv_kv = upstream_voltage / 1000
        vn_lv_kv = downstream_voltage / 1000
        vector_group = "Dyn"

        tx_idx = pp.create_transformer_from_parameters(
            bus_branch_network,
            # NOTE: We are assigning busses based on upstream/downstream instead of hv/lv
            # to handle regulators and step-up transformers.
            hv_bus=upstream_tn.index,
            lv_bus=downstream_tn.index,
            sn_mva=sn_mva,
            vn_hv_kv=vn_hv_kv,
            vn_lv_kv=vn_lv_kv,
            vk_percent=0.01,
            vkr_percent=0.005,
            pfe_kw=0,
            i0_percent=0,
            vector_group=vector_group,
            name=power_transformer.name
        )

        mapped_elements[f"trafo:{tx_idx}"] = PpElement(tx_idx, "trafo")

        return mapped_elements

    def _create_downstream_lv_network_and_bus(
            self,
            bus_branch_network: pp.pandapowerNet,
            power_transformer: PowerTransformer,
            downstream_voltage: int,
            mapped_elements: Dict[str, PpElement]
    ) -> PpElement:
        # Create Bus
        coord: Tuple[float, float] = [(p.x_position, p.y_position) for p in power_transformer.location.points][0]
        traced_phases = next(power_transformer.terminals).traced_phases
        p_keys, q_keys = _get_active_phase_keys(traced_phases)

        bus_idx = pp.create_bus(
            bus_branch_network,
            vn_kv=downstream_voltage / 1000,
            name=f"{power_transformer.name}_bus",
            geodata=coord
        )
        bus_element = PpElement(bus_idx, "bus")

        # Create Load
        p, q = self.load_provider(power_transformer)

        if self.asymmetric_load:
            load_idx = pp.create_asymmetric_load(
                bus_branch_network,
                bus=bus_idx,
                name=f"{power_transformer.name}_load",
                **{k: p / 1000000 for k in p_keys},
                **{k: q / 1000000 for k in q_keys}
            )
        else:
            load_idx = pp.create_load(
                bus_branch_network,
                bus=bus_idx,
                p_mw=p / 1000000,
                q_mvar=q / 1000000,
                name=f"{power_transformer.name}_load",
            )

        # Create PV
        p, q = self.pec_load_provider(power_transformer)

        if self.asymmetric_load:
            pv_load_idx = pp.create_asymmetric_sgen(
                bus_branch_network,
                bus=bus_idx,
                name=f"{power_transformer.name}_load",
                **{k: p / 1000000 for k in p_keys},
                **{k: q / 1000000 for k in q_keys}
            )
        else:
            pv_load_idx = pp.create_sgen(
                bus_branch_network,
                bus=bus_idx,
                p_mw=p / 1000000,
                q_mvar=q / 1000000,
                name=f"{power_transformer.name}_sgen",
            )

        mapped_elements[f"bus:{bus_idx}"] = bus_element
        mapped_elements[f"load:{load_idx}"] = PpElement(load_idx, "load")
        mapped_elements[f"sgen:{pv_load_idx}"] = PpElement(pv_load_idx, "sgen")

        return bus_element

    def energy_source_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            energy_source: EnergySource,
            connected_topological_node: PpElement,
            node_breaker_network: NetworkService
    ) -> Dict[str, PpElement]:
        ext_grid_idx = pp.create_ext_grid(
            bus_branch_network,
            bus=connected_topological_node.index,
            vm_pu=self.vm_pu,
            name=energy_source.name
        )

        return {f"ext_grid:{ext_grid_idx}": PpElement(ext_grid_idx, "ext_grid")}

    def energy_consumer_creator(
            self, bus_branch_network: pp.pandapowerNet,
            energy_consumer: EnergyConsumer,
            connected_topological_node: PpElement,
            node_breaker_network: NetworkService
    ) -> Dict[str, PpElement]:
        if self.asymmetric_load:
            return self._create_asymmetric_load(
                bus_branch_network,
                energy_consumer,
                connected_topological_node,
                node_breaker_network
            )
        else:
            return self._create_load(
                bus_branch_network,
                energy_consumer,
                connected_topological_node,
                node_breaker_network
            )

    def _create_load(
            self,
            bus_branch_network: pp.pandapowerNet,
            energy_consumer: EnergyConsumer,
            connected_topological_node: PpElement,
            node_breaker_network: NetworkService
    ) -> Dict[str, PpElement]:
        p, q = self.load_provider(energy_consumer)
        load_idx = pp.create_load(
            bus_branch_network,
            bus=connected_topological_node.index,
            p_mw=p / 1000000,
            q_mvar=q / 1000000,
            name=energy_consumer.name
        )

        return {f"load:{load_idx}": PpElement(load_idx, "load")}

    def _create_asymmetric_load(
            self,
            bus_branch_network: pp.pandapowerNet,
            energy_consumer: EnergyConsumer,
            connected_topological_node: PpElement,
            node_breaker_network: NetworkService
    ) -> Dict[str, PpElement]:
        p, q = self.load_provider(energy_consumer)
        p_keys, q_keys = _get_active_phase_keys(next(energy_consumer.terminals).traced_phases)
        load_idx = pp.create_asymmetric_load(
            bus_branch_network,
            bus=connected_topological_node.index,
            name=energy_consumer.name,
            **{k: p / 1000000 for k in p_keys},
            **{k: q / 1000000 for k in q_keys}
        )

        return {f"load:{load_idx}": PpElement(load_idx, "load")}

    def power_electronics_connection_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            power_electronics_connection: PowerElectronicsConnection,
            connected_topological_node: PpElement,
            node_breaker_network: NetworkService,
    ) -> Dict[str, PpElement]:
        if self.asymmetric_load:
            return self._create_asymmetric_sgen(
                bus_branch_network,
                power_electronics_connection,
                connected_topological_node,
                node_breaker_network
            )
        else:
            return self._create_sgen(
                bus_branch_network,
                power_electronics_connection,
                connected_topological_node,
                node_breaker_network
            )

    def _create_sgen(
            self,
            bus_branch_network: pp.pandapowerNet,
            power_electronics_connection: PowerElectronicsConnection,
            connected_topological_node: PpElement,
            node_breaker_network: NetworkService
    ) -> Dict[str, PpElement]:
        p, q = self.pec_load_provider(power_electronics_connection)
        load_idx = pp.create_sgen(
            bus_branch_network,
            bus=connected_topological_node.index,
            p_mw=p / 1000000,
            q_mvar=q / 1000000,
            name=power_electronics_connection.name
        )

        return {f"sgen:{load_idx}": PpElement(load_idx, "sgen")}

    def _create_asymmetric_sgen(
            self,
            bus_branch_network: pp.pandapowerNet,
            power_electronics_connection: PowerElectronicsConnection,
            connected_topological_node: PpElement,
            node_breaker_network: NetworkService
    ) -> Dict[str, PpElement]:
        p, q = self.pec_load_provider(power_electronics_connection)
        p_keys, q_keys = _get_active_phase_keys(next(power_electronics_connection.terminals).traced_phases)
        load_idx = pp.create_asymmetric_sgen(
            bus_branch_network,
            bus=connected_topological_node.index,
            name=power_electronics_connection.name,
            **{k: p / 1000000 for k in p_keys},
            **{k: q / 1000000 for k in q_keys}
        )

        return {f"sgen:{load_idx}": PpElement(load_idx, "sgen")}

    def has_negligible_impedance(self, ce: ConductingEquipment) -> bool:
        if isinstance(ce, AcLineSegment):
            if ce.length == 0 or ce.per_length_sequence_impedance.r == 0:
                return True

            if ce.length * ce.per_length_sequence_impedance.r < 0.005:
                return True

            return False
        if isinstance(ce, Switch):
            return not ce.is_open()
        if isinstance(ce, Junction) or isinstance(ce, BusbarSection):
            return True
        return False

    def validator_creator(self) -> PandaPowerNetworkValidator:
        return PandaPowerNetworkValidator(logger=self.logger)


def _get_active_phase_keys(traced_phases: TracedPhases) -> Tuple[List[str], List[str]]:
    p_keys = []
    q_keys = []

    for nominal_phase in PhaseCode.ABC.single_phases:
        if traced_phases.phase_normal(nominal_phase) != SinglePhaseKind.NONE:
            p_key = P_OPTIONS.get(nominal_phase)
            q_key = Q_OPTIONS.get(nominal_phase)
            if p_key:
                p_keys.append(p_key)
            if q_key:
                q_keys.append(q_key)

    return p_keys, q_keys


def _create_id_from_terminals(ts: Iterable[Terminal]):
    "_".join(sorted((t.mrid for t in ts)))
    pass
