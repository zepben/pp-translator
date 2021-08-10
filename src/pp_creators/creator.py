#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import logging
from typing import FrozenSet, Tuple, Iterable, List, Optional, Dict

import pandapower as pp
from zepben.evolve import Terminal, NetworkService, AcLineSegment, PowerTransformer, EnergySource, EnergyConsumer, \
    BusBranchNetworkCreator, \
    PowerTransformerEnd, ConductingEquipment, PowerElectronicsConnection

__all__ = ["PandaPowerNetworkCreator"]

from pp_creators.utils import get_upstream_end_to_tns
from pp_creators.validators.validator import PandaPowerNetworkValidator


class PandaPowerNetworkCreator(
    BusBranchNetworkCreator[pp.pandapowerNet, int, int, int, int, int, int, PandaPowerNetworkValidator]):

    def __init__(self, *, vm_pu: float = 1.0, logger: logging.Logger):
        self.vm_pu = vm_pu
        self.logger = logger

    def bus_branch_network_creator(self, node_breaker_network: NetworkService) -> pp.pandapowerNet:
        bus_branch_network = pp.create_empty_network()
        return bus_branch_network

    def topological_node_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            base_voltage: Optional[int],
            collapsed_conducting_equipment: FrozenSet[ConductingEquipment],
            border_terminals: FrozenSet[Terminal],
            inner_terminals: FrozenSet[Terminal],
            node_breaker_network: NetworkService
    ) -> Tuple[int, int]:
        bus_idx = pp.create_bus(
            bus_branch_network,
            vn_kv=base_voltage / 1000,
            name=f"bus_{_create_id_from_terminals(border_terminals)}"
        )
        return bus_idx, bus_idx

    def topological_branch_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            connected_topological_nodes: Tuple[int, int],
            length: Optional[float],
            collapsed_ac_line_segments: FrozenSet[AcLineSegment],
            border_terminals: FrozenSet[Terminal],
            inner_terminals: FrozenSet[Terminal],
            node_breaker_network: NetworkService
    ) -> Tuple[int, int]:
        line_idx = pp.create_line(
            bus_branch_network,
            from_bus=connected_topological_nodes[0],
            to_bus=connected_topological_nodes[1],
            length_km=length / 1000,
            name=f"line_{_create_id_from_terminals(border_terminals)}",
            std_type="NAYY 4x50 SE"
        )
        return line_idx, line_idx

    def power_transformer_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            power_transformer: PowerTransformer,
            ends_to_topological_nodes: List[Tuple[PowerTransformerEnd, Optional[int]]],
            node_breaker_network: NetworkService
    ) -> Dict[str, int]:
        upstream_end, upstream_tn = get_upstream_end_to_tns(ends_to_topological_nodes)[0]
        downstream_tn = [tn for (end, tn) in ends_to_topological_nodes if tn != upstream_tn][0]

        tx_idx = pp.create_transformer(
            bus_branch_network,
            hv_bus=upstream_tn,
            lv_bus=downstream_tn,
            std_type="0.4 MVA 20/0.4 kV",
            name=power_transformer.name
        )
        return {f"line:{tx_idx}": tx_idx}

    def energy_source_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            energy_source: EnergySource,
            connected_topological_node: int,
            node_breaker_network: NetworkService
    ) -> Dict[str, int]:
        ext_grid_idx = pp.create_ext_grid(
            bus_branch_network,
            bus=connected_topological_node,
            vm_pu=self.vm_pu,
            name=energy_source.name
        )
        return {f"ext_grid:{ext_grid_idx}": ext_grid_idx}

    def energy_consumer_creator(
            self, bus_branch_network: pp.pandapowerNet,
            energy_consumer: EnergyConsumer,
            connected_topological_node: int,
            node_breaker_network: NetworkService
    ) -> Dict[str, int]:
        load_idx = pp.create_load(
            bus_branch_network,
            bus=connected_topological_node,
            p_mw=energy_consumer.p / 1000000,
            q_mvar=energy_consumer.q / 1000000,
            name=energy_consumer.name
        )
        return {f"load:{load_idx}": load_idx}

    def power_electronics_connection_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            power_electronics_connection: PowerElectronicsConnection,
            connected_topological_node: int,
            node_breaker_network: NetworkService,
    ) -> Dict[str, int]:
        load_idx = pp.create_load(
            bus_branch_network,
            bus=connected_topological_node,
            p_mw=power_electronics_connection.p / 1000000,
            q_mvar=power_electronics_connection.q / 1000000,
            name=power_electronics_connection.name
        )
        return {f"load:{load_idx}": load_idx}

    def validator_creator(self) -> PandaPowerNetworkValidator:
        return PandaPowerNetworkValidator(logger=self.logger)


def _create_id_from_terminals(ts: Iterable[Terminal]):
    "_".join(sorted((t.mrid for t in ts)))
    pass
