#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from typing import FrozenSet, Tuple, List, Optional, Set, Dict

from zepben.evolve import Terminal, NetworkService, AcLineSegment, PowerTransformer, EnergyConsumer, \
    PowerTransformerEnd, ConductingEquipment, \
    PowerElectronicsConnection, BusBranchNetworkCreator, IdentifiedObject, BusBranchNetworkCreationValidator, \
    EnergySource

from pp_creators.utils import get_upstream_end_to_tns

__all__ = ["NetworkError", "NetworkErrors", "ErrorAggregator"]


class NetworkError:

    def __init__(self, description: str, ios: Set[IdentifiedObject] = None):
        self.description = description
        self.ios = set() if ios is None else ios


class NetworkErrors:

    def __init__(self):
        self.errors: Dict[str, NetworkError] = {
            "missing_voltage": NetworkError("Equipment has no voltage"),
            "acls_missing_length": NetworkError("AcLineSegment has no length"),
            "pt_no_upstream_terminal": NetworkError("PowerTransformer has no upstream terminal"),
            "pt_multiple_upstream_terminals": NetworkError("PowerTransformer has multiple upstream terminals"),
            "pt_no_downstream_terminal": NetworkError("PowerTransformer has no downstream terminal"),
            "pt_multiple_downstream_terminals": NetworkError("PowerTransformer has multiple downstream terminals"),
            "pt_terminals_and_end_terminals_not_matching": NetworkError(
                "PowerTransformer terminals do not match the ends' terminals"),
            "pt_end_missing_voltage": NetworkError("PowerTransformer end has no voltage")
        }
        self.count: int = 0

    def get_errors(self) -> List[NetworkError]:
        return sorted(self.errors.values(), key=lambda val: len(val.ios), reverse=True)

    def add_errors(self, new_errors: 'NetworkErrors'):
        for k, err in new_errors.errors.items():
            for io in err.ios:
                self.errors[k].ios.add(io)

    def get_inc(self):
        val = self.count
        self.count += 1
        return val


class PermissiveValidator(BusBranchNetworkCreationValidator[NetworkErrors, int, int, int, int, int, int]):

    def is_valid_network_data(self, node_breaker_network: NetworkService) -> bool:
        return True

    def is_valid_topological_node_data(self, *arg, **kargs) -> bool:
        return True

    def is_valid_topological_branch_data(self, *arg, **kargs) -> bool:
        return True

    def is_valid_power_transformer_data(self, *arg, **kargs) -> bool:
        return True

    def is_valid_energy_source_data(self, *arg, **kargs) -> bool:
        return True

    def is_valid_energy_consumer_data(self, *arg, **kargs) -> bool:
        return True

    def is_valid_power_electronics_connection_data(self, *arg, **kargs) -> bool:
        return True


class ErrorAggregator(BusBranchNetworkCreator[NetworkErrors, int, int, int, int, int, int, PermissiveValidator]):

    def bus_branch_network_creator(self, node_breaker_network: NetworkService) -> NetworkErrors:
        return NetworkErrors()

    def topological_node_creator(
            self,
            bus_branch_network: NetworkErrors,
            base_voltage: Optional[int],
            collapsed_conducting_equipment: FrozenSet[ConductingEquipment],
            border_terminals: FrozenSet[Terminal],
            inner_terminals: FrozenSet[Terminal],
            node_breaker_network: NetworkService
    ) -> Tuple[int, int]:
        if base_voltage is None:
            for cce in collapsed_conducting_equipment:
                if cce.base_voltage is None:
                    bus_branch_network.errors["missing_voltage"].ios.add(cce)

            for t in border_terminals:
                if t.conducting_equipment.base_voltage is None:
                    bus_branch_network.errors["missing_voltage"].ios.add(t.conducting_equipment)

            for t in inner_terminals:
                if t.conducting_equipment.base_voltage is None:
                    bus_branch_network.errors["missing_voltage"].ios.add(t.conducting_equipment)

        count = bus_branch_network.get_inc()
        return count, count

    def topological_branch_creator(
            self,
            bus_branch_network: NetworkErrors,
            connected_topological_nodes: Tuple[int, int],
            length: Optional[float],
            collapsed_ac_line_segments: FrozenSet[AcLineSegment],
            border_terminals: FrozenSet[Terminal],
            inner_terminals: FrozenSet[Terminal],
            node_breaker_network: NetworkService
    ) -> Tuple[int, int]:
        for acls in collapsed_ac_line_segments:
            if acls.length is None:
                bus_branch_network.errors["acls_missing_length"].ios.add(acls)
        count = bus_branch_network.get_inc()
        return count, count

    def power_transformer_creator(
            self,
            bus_branch_network: NetworkErrors,
            power_transformer: PowerTransformer,
            ends_to_topological_nodes: List[Tuple[PowerTransformerEnd, Optional[int]]],
            node_breaker_network: NetworkService
    ) -> Dict[int, int]:
        upstream_end_to_tns = get_upstream_end_to_tns(ends_to_topological_nodes)
        upstream_tns = [tn for (end, tn) in upstream_end_to_tns]
        downstream_tns = [tn for (end, tn) in ends_to_topological_nodes if tn not in upstream_tns and tn is not None]
        if len(upstream_tns) == 0:
            bus_branch_network.errors["pt_no_upstream_terminal"].ios.add(power_transformer)
        if len(upstream_tns) > 1:
            bus_branch_network.errors["pt_multiple_upstream_terminals"].ios.add(power_transformer)
        if len(downstream_tns) == 0:
            bus_branch_network.errors["pt_no_downstream_terminal"].ios.add(power_transformer)
        if len(downstream_tns) > 1:
            bus_branch_network.errors["pt_multiple_downstream_terminals"].ios.add(power_transformer)

        ends_missing_voltage = [end for end in power_transformer.ends if end.rated_u is None]
        if len(ends_missing_voltage):
            bus_branch_network.errors["pt_end_missing_voltage"].ios.add(power_transformer)

        end_terminals = {end.terminal.mrid for end in power_transformer.ends if end.terminal is not None}
        pt_terminals = {t.mrid for t in power_transformer.terminals if t is not None}
        symm_diff = end_terminals ^ pt_terminals
        if len(symm_diff) != 0:
            bus_branch_network.errors["pt_terminals_and_end_terminals_not_matching"].ios.add(power_transformer)

        count = bus_branch_network.get_inc()
        return {count: count}

    def energy_source_creator(self, bus_branch_network: NetworkErrors, energy_source: EnergySource,
                              connected_topological_node: int, node_breaker_network: NetworkService) -> Dict[int, int]:
        count = bus_branch_network.get_inc()
        return {count: count}

    def energy_consumer_creator(
            self, bus_branch_network: NetworkErrors,
            energy_consumer: EnergyConsumer,
            connected_topological_node: int,
            node_breaker_network: NetworkService
    ) -> Dict[int, int]:
        count = bus_branch_network.get_inc()
        return {count: count}

    def power_electronics_connection_creator(
            self,
            bus_branch_network: NetworkErrors,
            power_electronics_connection: PowerElectronicsConnection,
            connected_topological_node: int,
            node_breaker_network: NetworkService,
    ) -> Dict[int, int]:
        count = bus_branch_network.get_inc()
        return {count: count}

    def validator_creator(self) -> PermissiveValidator:
        return PermissiveValidator()
