import pandapower as pp
import pandas as pd
from zepben.evolve import NetworkService, ConnectivityNode, PowerElectronicsConnection, \
    PowerTransformer, Switch, Connector, Conductor, Feeder, Terminal, ConductingEquipment, EnergyConsumer


class EvolveToPandaPowerMap:
    def __init__(self, network_service: NetworkService, pp_net: pp.pandapowerNet, feeder_mrid):
        self.network_service: NetworkService = network_service
        self.feeder_mrid = feeder_mrid
        self.pp_net: pp.pandapowerNet = pp_net
        self.connectivity_nodes_to_buses()
        self.connectors_to_buses()
        self.switch_to_bus()
        self.conductors_to_lines()
        self.head_terminal_to_ext_grid()
        self.power_transformers_to_trafo()
        self.energy_consumers_to_load()

    def energy_consumers_to_load(self):
        print(f'Mapping Energy Consumers  to Loads')
        for ec in self.network_service.objects(EnergyConsumer):
            ec: EnergyConsumer = ec
            bus = self.get_bus_indexes_by_cond_eq(ec)[0]
            pp.create_load(net=self.pp_net, bus=bus, p_mw=ec.p / 1000000, q_mvar=ec.q / 1000000)

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
                            vn_kv = end.rated_u / 1000
                else:
                    nominal_voltages = []
                    if terminal.conducting_equipment.base_voltage is not None:
                        nominal_voltages.append(terminal.conducting_equipment.base_voltage.nominal_voltage)
                    if len(nominal_voltages) == 0:
                        raise Exception(
                            f'Any nominal_voltage was found for the connectivity node {cn} ')
                    if all(x == nominal_voltages[0] for x in nominal_voltages):
                        vn_kv = nominal_voltages[0] / 1000
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

    def switch_to_switch(self):
        print(f'Mapping Switch to Switch')
        for sw in self.network_service.objects(Switch):
            sw: Switch = sw
            if sw.num_terminals() == 2:
                [from_bus, to_bus] = self.get_bus_indexes_by_cond_eq(sw)
                if sw.is_open() is False:
                    pp.create_switch(self.pp_net, bus=from_bus, element=to_bus, name=str(sw.mrid), closed=True, et="b")
                else:
                    pp.create_switch(self.pp_net, bus=from_bus, element=to_bus, name=str(sw.mrid), closed=False, et="b")
            elif sw.num_terminals() > 3:
                raise Exception(f'Switches with more than 2 terminals not supported. {sw}')
            else:
                print(f'Switch with only one terminal neglected. {sw}')
                pass

    def switch_to_bus(self):
        print(f'Mapping Switch to Bus')
        for sw in self.network_service.objects(Switch):
            sw: Switch = sw
            if sw.base_voltage is not None:
                vn_kv = sw.base_voltage.nominal_voltage
                pp.create_bus(self.pp_net, vn_kv=vn_kv, name=str(sw.mrid))
            else:
                raise Exception(f'None nominal_voltage was found for the junction {sw}')

    def conductors_to_lines(self):
        print(f'Mapping Conductors to Lines')
        for conductor in self.network_service.objects(Conductor):
            conductor: Conductor = conductor
            [from_bus, to_bus] = self.get_bus_indexes_by_cond_eq(conductor)
            if conductor.base_voltage.nominal_voltage < 1000:
                std_type = "NAYY 4x50 SE"
            elif conductor.base_voltage.nominal_voltage < 12000:
                # Medium Voltage
                std_type = "NA2XS2Y 1x95 RM/25 12/20 kV"
            else:
                # High Voltage
                std_type = "N2XS(FL)2Y 1x120 RM/35 64/110 kV"
            pp.create_line(self.pp_net, from_bus=from_bus, to_bus=to_bus,
                           length_km=conductor.length / 1000,
                           std_type=std_type)

    def power_transformers_to_trafo(self):
        print(f'Mapping PowerTransformers to Transformers')
        for transformer in self.network_service.objects(PowerTransformer):
            transformer: PowerTransformer = transformer
            if len(list(transformer.ends)):
                if transformer.num_terminals() == 2:
                    [hv_bus, lv_bus] = self.get_bus_indexes_by_cond_eq(transformer)
                    pp.create_transformer(self.pp_net, hv_bus=hv_bus, lv_bus=lv_bus, std_type="0.4 MVA 20/0.4 kV")
                else:
                    print(f'Transformer with num_terminals  different to 2 neglected. {transformer}')
            else:
                raise Exception(f'Mapping of non two winding transformers is not supported')

    def head_terminal_to_ext_grid(self):
        print(f'Creating External Grid')
        feeder = self.network_service.get(self.feeder_mrid)
        if isinstance(feeder, Feeder):
            head_terminal: Terminal = feeder.normal_head_terminal
            print(f'Head Terminal found: {head_terminal}')
        else:
            raise Exception(f'Head Terminal for feeder mRID:  {self.feeder_mrid} not found. Ext Grid creation failed.')
        cn: ConnectivityNode = head_terminal.connectivity_node
        bus_name = cn.mrid
        bus = self.get_bus_index_by_name(bus_name)
        pp.create_ext_grid(self.pp_net, vm_pu=1.02, va_degree=0, name=feeder.mrid, bus=bus)
        print(f'External Grid created.')

    def get_bus_indexes_by_cond_eq(self, conducting_equipment: ConductingEquipment):
        index_bus = []
        if conducting_equipment.num_terminals() == 2:
            for terminal in list(conducting_equipment.terminals):
                bus_name = terminal.connectivity_node.mrid
                index_bus.append(self.get_bus_index_by_name(bus_name))
            return index_bus
        elif conducting_equipment.num_terminals() == 1:
            bus_name = conducting_equipment.get_terminal_by_sn(1).connectivity_node.mrid
            index_bus.append(self.get_bus_index_by_name(bus_name))
            return index_bus
        else:
            raise Exception(f'Number of terminals of the ConductingEquipment is not 2. Check {conducting_equipment}')

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
