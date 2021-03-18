import pandapower as pp
from zepben.evolve import AcLineSegment, NetworkService, ConnectivityNode, PowerElectronicsConnection, \
    PowerTransformer, Junction, Switch


class EvolveToPandaPowerMap:
    def __init__(self, evolve_service: NetworkService, pp_net: pp.pandapowerNet):
        self.evolve_service: NetworkService = evolve_service
        self.pp_net: pp.pandapowerNet = pp_net
        self.connectivity_nodes_to_buses()
        self.junctions_to_buses()
        self.switches_to_buses()

    def connectivity_nodes_to_buses(self):
        print(f'Mapping Connectivity Nodes to Buses')
        for obj in self.evolve_service.objects(ConnectivityNode):
            cn: ConnectivityNode = obj
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

    def junctions_to_buses(self):
        print(f'Mapping Junctions to Buses')
        for obj in self.evolve_service.objects(Junction):
            junction: Junction = obj
            if junction.base_voltage is not None:
                vn_kv = junction.base_voltage.nominal_voltage
                pp.create_bus(self.pp_net, vn_kv=vn_kv, name=str(junction.mrid))
            else:
                raise Exception(f'None nominal_voltage was found for the junction {junction}')

    def switches_to_buses(self):
        print(f'Mapping Switches to Buses')
        for obj in self.evolve_service.objects(Switch):
            sw: Switch = obj
            if sw.is_open() is False:
                if sw.base_voltage is not None:
                    vn_kv = sw.base_voltage.nominal_voltage
                    pp.create_bus(self.pp_net, vn_kv=vn_kv, name=str(sw.mrid))
                else:
                    raise Exception(f'None nominal_voltage was found for the junction {sw}')

    def ac_line_segments_to_lines(self):
        for acls in self.evolve_service.objects(AcLineSegment):
            pp.create_line(self.pp_net, from_bus=1, to_bus=1)  # TODO: add input busses
