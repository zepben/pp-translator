import pandapower as pp
import pandas as pd
from zepben.evolve import NetworkService, ConnectivityNode, PowerElectronicsConnection, \
    PowerTransformer, Switch, Connector, Conductor


class EvolveToPandaPowerMap:
    def __init__(self, network_service: NetworkService, pp_net: pp.pandapowerNet):
        self.network_service: NetworkService = network_service
        self.pp_net: pp.pandapowerNet = pp_net
        self.connectivity_nodes_to_buses()
        self.connectors_to_buses()
        self.switches_to_buses()
        self.conductors_to_lines()

    def connectivity_nodes_to_buses(self):
        print(f'Mapping Connectivity Nodes to Buses')
        for cn in self.network_service.objects(ConnectivityNode):
            cn: ConnectivityNode = cn
            vn_kv = None
            for terminal in list(cn.terminals):
                if isinstance(terminal.conducting_equipment, PowerElectronicsConnection):
                    # TODO: Fix when the PowerElectronicsConnection has an associated Base Voltage instance
                    pass
                elif isinstance(terminal.conducting_equipment, PowerTransformer):
                    for end in list(terminal.conducting_equipment.ends):
                        if end.rated_u == 0:
                            raise Exception(
                                f' rated_u = 0 was found for the {end}, associated to {cn}')
                        else:
                            vn_kv = end.rated_u
                else:
                    nominal_voltages = []
                    if terminal.conducting_equipment.base_voltage is not None:
                        nominal_voltages.append(terminal.conducting_equipment.base_voltage.nominal_voltage)
                    if len(nominal_voltages) == 0:
                        raise Exception(
                            f'Any nominal_voltage was found for the connectivity node {cn} ')
                    if all(x == nominal_voltages[0] for x in nominal_voltages):
                        vn_kv = nominal_voltages[0]
                    else:
                        raise Exception(f'Different nominal_voltages in the Conducting Equipments '
                                        f'connected to the Connectivity Node: {cn}')
            pp.create_bus(self.pp_net, vn_kv=vn_kv, name=str(cn.mrid))

    def connectors_to_buses(self):
        print(f'Mapping Connectors to Buses')
        for connector in self.network_service.objects(Connector):
            connector: Connector = connector
            if connector.base_voltage is not None:
                vn_kv = connector.base_voltage.nominal_voltage
                pp.create_bus(self.pp_net, vn_kv=vn_kv, name=str(connector.mrid))
            else:
                raise Exception(f'None nominal_voltage was found for the junction {connector}')

    def switches_to_buses(self):
        print(f'Mapping closed Switches to Buses')
        for sw in self.network_service.objects(Switch):
            sw: Switch = sw
            if sw.is_open() is False:
                if sw.base_voltage is not None:
                    vn_kv = sw.base_voltage.nominal_voltage
                    pp.create_bus(self.pp_net, vn_kv=vn_kv, name=str(sw.mrid))
                else:
                    raise Exception(f'None nominal_voltage was found for the junction {sw}')

    def conductors_to_lines(self):
        print(f'Mapping Conductors to Lines')
        for conductor in self.network_service.objects(Conductor):
            conductor: Conductor = conductor
            [from_bus, to_bus] = self.get_bus_indexes_by_conductor(conductor)
            if conductor.base_voltage.nominal_voltage < 1000:
                std_type = "NAYY 4x50 SE"
            elif conductor.base_voltage.nominal_voltage < 12000:
                # Medium Voltage
                std_type = "NA2XS2Y 1x95 RM/25 12/20 kV"
            else:
                # High Voltage
                std_type = "N2XS(FL)2Y 1x120 RM/35 64/110 kV"
            pp.create_line(self.pp_net, from_bus=from_bus, to_bus=to_bus,
                           length_km=conductor.length,
                           std_type=std_type)

    def head_terminal_to_ext_grid(self):
        print(f'Mapping Head Terminal to External Grid')


    def get_bus_indexes_by_conductor(self, conductor: Conductor):
        index_bus = []
        if conductor.num_terminals() == 2:
            for terminal in list(conductor.terminals):
                bus_name = terminal.connectivity_node.mrid
                index_bus.append(self.get_bus_index_by_name(bus_name))
            return index_bus
        else:
            raise Exception(f'Number of terminals of the Conductor is not 2')

    def get_bus_index_by_name(self, bus_name):
        bus_df: pd.DataFrame = self.pp_net.bus
        index = bus_df.index
        condition = bus_df['name'] == bus_name
        list_buses = bus_df[condition].name.values
        if len(list_buses) == 1:
            bus_index = index[condition].values[0]
        else:
            raise Exception(f'Number of Buses with name: {bus_name} is more than 1')
        return int(bus_index)
