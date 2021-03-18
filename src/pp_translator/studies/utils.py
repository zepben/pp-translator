from collections import defaultdict

import pandapower as pp
from zepben.evolve import NetworkService, DiagramService, Diagram, \
    DiagramStyle, BaseVoltage, PositionPoint, Location, Feeder, EnergySource, PowerTransformerInfo, DiagramObject, \
    AcLineSegment, DiagramObjectStyle, ConductingEquipment, Junction, EnergyConsumer, \
    PowerTransformer, DiagramObjectPoint, ConnectivityNode, Conductor, Connector, Switch

from pp_translator.mappers.evolve_to_pandapower.mappings import EvolveToPandaPowerMap


def create_pp_model(network_service) -> pp.pandapowerNet:
    pp_net = pp.create_empty_network()
    return EvolveToPandaPowerMap(evolve_service=network_service, pp_net=pp_net).pp_net


class SimpleNodeBreakerFeeder:

    def __init__(self, breaker_is_open=False):
        self.breaker_is_open = breaker_is_open
        self.network_service: NetworkService = NetworkService()
        self.diagram_service: DiagramService = DiagramService()
        self.diagram: Diagram = Diagram(diagram_style=DiagramStyle.GEOGRAPHIC)
        self._create_network_service()
        self._create_diagram_service()

    def _create_network_service(self):
        # Create BaseVoltages
        bv_hv: BaseVoltage = BaseVoltage(mrid="20kV", nominal_voltage=20000, name="20kV")
        bv_lv: BaseVoltage = BaseVoltage(mrid="415V", nominal_voltage=400, name="415V")
        self.network_service.add(bv_hv)
        self.network_service.add(bv_lv)
        # Create Locations for buses
        point1 = PositionPoint(x_position=149.12791965570293, y_position=-35.277592101000934)
        point2 = PositionPoint(x_position=149.12779472660375, y_position=-35.278183862759285)
        loc1 = Location().add_point(point1)
        loc2 = Location().add_point(point2)
        # Create connectivity_nodes
        cn1 = ConnectivityNode()
        cn1.mrid = "cn1"
        cn2 = ConnectivityNode()
        cn2.mrid = "cn2"
        cn3 = ConnectivityNode()
        cn3.mrid = "cn3"
        cn4 = ConnectivityNode()
        cn4.mrid = "cn4"
        # Create EnergySource
        energy_source: EnergySource = self.network_service.create_energy_source(cn=cn1, base_voltage=bv_hv,
                                                                                voltage_magnitude=1.02 * bv_hv.nominal_voltage,
                                                                                name="Grid Connection", location=loc1)
        # TODO: Replace  createEnergySource with creation of EquivalentInjection
        # Create Feeder
        fdr = Feeder(normal_head_terminal=energy_source.get_terminal_by_sn(1))
        self.network_service.add(fdr)
        # Create Transformer
        self.network_service.create_two_winding_power_transformer(cn1=cn1, cn2=cn2, name="Trafo", location=loc1,
                                                                  asset_info=PowerTransformerInfo())
        # TODO: Associate the PowerTransformerInfo() to th PowerTransformer instance
        # TODO: Add ptInfo= self.network_service.getAvailablePowerTransformerInfo("0.4 MVA 20/0.4 kV")
        # Create Breaker
        breaker = self.network_service.create_breaker(cn1=cn2, cn2=cn3, base_voltage=bv_lv, mrid='sw1')
        breaker.set_open(self.breaker_is_open)
        # Create location for the Line
        line_location = Location().add_point(point1).add_point(point2)
        self.network_service.add(line_location)
        # Create Line
        self.network_service.create_ac_line_segment(cn1=cn3, cn2=cn4, name="Line",
                                                    length=100., base_voltage=bv_lv,
                                                    location=line_location)
        # Create EnergyConsumer
        self.network_service.create_energy_consumer(cn=cn3, p=100000., q=50000., name="Load",
                                                    location=loc2,
                                                    base_voltage=bv_lv)

    def _create_diagram_service(self):
        self.diagram_service.add(self.diagram)
        self._add_diagram_objects()
        # TODO: In ?voltages geo view the acls does not appear.

    def _add_diagram_objects(self):
        # Add DiagramObject for ConductingEquipments
        ce_list = self.network_service.objects(ConductingEquipment)
        for ce in ce_list:
            diagram_object_mapping = defaultdict(
                lambda: DiagramObject(identified_object_mrid=ce.mrid, style=DiagramObjectStyle.JUNCTION,
                                      diagram=self.diagram))
            if type(ce) == Junction:
                diagram_object = DiagramObject(identified_object_mrid=ce.mrid,
                                               style=DiagramObjectStyle.JUNCTION,
                                               diagram=self.diagram)
            elif type(ce) == EnergySource:
                diagram_object = DiagramObject(identified_object_mrid=ce.mrid,
                                               style=DiagramObjectStyle.ENERGY_SOURCE,
                                               diagram=self.diagram)
            elif type(ce) == EnergyConsumer:
                diagram_object = DiagramObject(identified_object_mrid=ce.mrid,
                                               style=DiagramObjectStyle.USAGE_POINT,
                                               diagram=self.diagram)
            elif type(ce) == PowerTransformer:
                diagram_object = DiagramObject(identified_object_mrid=ce.mrid,
                                               style=DiagramObjectStyle.DIST_TRANSFORMER,
                                               diagram=self.diagram)

            elif type(ce) == AcLineSegment:
                diagram_object = self._add_diagram_objects_to_ac_line_segment(ce)
            else:
                diagram_object = DiagramObject(identified_object_mrid=ce.mrid,
                                               style=DiagramObjectStyle.JUNCTION,
                                               diagram=self.diagram)
            self.diagram.add_object(diagram_object)
            self.diagram_service.add(diagram_object)

    def _add_diagram_objects_to_ac_line_segment(self, ac_line_segment: AcLineSegment):
        # Create DiagramObject for AcLineSegments
        diagram_object = DiagramObject(diagram=self.diagram)
        diagram_object.mrid = ac_line_segment.mrid + "-do"
        diagram_object.style = DiagramObjectStyle.CONDUCTOR_LV
        diagram_object.diagram = self.diagram
        for position_point in ac_line_segment.location.points:
            diagram_point = DiagramObjectPoint(x_position=position_point.x_position,
                                               y_position=position_point.y_position)
            diagram_object.add_point(diagram_point)
        return diagram_object


def network_service_inventory(network_service: NetworkService):
    print('Network Service Inventory:')
    print(f' - BaseVoltages: {list(network_service.objects(BaseVoltage))}')
    cns_count = len(list(network_service.objects(ConnectivityNode)))
    print(f' - Connectivity Nodes: {cns_count}')
    connector_count = len(list(network_service.objects(Connector)))
    print(f' - Connectors: {connector_count}')
    list_sw = list(network_service.objects(Switch))
    sw_count = len(list_sw)
    print(f' - Switches: {sw_count}')
    closed_switches_count = len([sw for sw in list_sw if sw.is_open() is False])
    print(f' - Closed Switches: {closed_switches_count}')
    print(f' - ConnectivityNodes + Junctions + closed Switches: '
          f'{connector_count + cns_count + closed_switches_count}')
    print(f' - Energy Consumers: {len(list(network_service.objects(EnergyConsumer)))}')
    print(f' - Conductors: {len(list(network_service.objects(Conductor)))}')
    print(f' - Power Transformers: {len(list(network_service.objects(PowerTransformer)))}')
