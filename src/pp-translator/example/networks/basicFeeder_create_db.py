#  Copyright 2020 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import argparse
import asyncio
import logging

from zepben.evolve import NetworkService, Breaker, Terminal, AcLineSegment, EnergySource, \
    EnergyConsumer, PerLengthSequenceImpedance, BaseVoltage, Location, PositionPoint, EnergySourcePhase, \
    PowerTransformerEnd, WindingConnection, PowerTransformer, VectorGroup, PhaseShuntConnectionKind, \
    EnergyConsumerPhase, RatioTapChanger, connect_async, ProducerClient, DiagramObject, DiagramService, Diagram, DiagramObjectStyle, DiagramObjectPoint
from zepben.evolve import PhaseCode, SinglePhaseKind, Feeder

logger = logging.getLogger(__name__)


def create_diagram():
    # Create a Diagram. To create a diagram you need to create a DiagramService()
    service = DiagramService()
    diagram = Diagram()
    service.add(diagram)
    pt_do = DiagramObject(diagram=diagram)
    pt_do.add_point(DiagramObjectPoint(149.10941149863936, 35.26964014234307))
    pt_do.identified_object_mrid = "PowerTransformer"
    pt_do.style = DiagramObjectStyle.DIST_TRANSFORMER
    service.add(pt_do)
    return service


def create_network():
    """
    Creates a small feeder based on https://bitbucket.org/zepben/cimdemo/src/master/lv_simple_net.png.
    :return: A NetworkService representing the feeder.
    """
    # Create the network. This will be used for sending all components of the feeder.
    network = NetworkService()

    # A network has multiple BaseVoltages, each used to represent the intended nominal voltage of equipment operating
    # on a segment of the network.
    bv_11kv = BaseVoltage(mrid='11kv', nominal_voltage=11000, name='11kV')

    # We must add the BaseVoltage to the network directly if any ConductingEquipment relies on it.
    network.add(bv_11kv)

    # There is no required order for adding equipment to the network, as long as its dependencies are satisfied.
    # In this example, we will start with the main EnergySource, working towards the leaf nodes in the network
    # in a depth-first traversal.

    # Geographic coordinates and location details are stored as a Location against any type extending
    # PowerSystemResource.
    energy_source_loc = Location(mrid='es-loc')
    energy_source_loc.add_point(PositionPoint(149.10905744704937, -35.2696664208097))
    network.add(energy_source_loc)

    # Create the EnergySource, specifying any desired parameters plus passing in our BaseVoltage, EnergySourcePhase's,
    # Location, and Terminals. Note that terminals accepts a list of Terminals, however an EnergySource only has one.
    energy_source = EnergySource(mrid='EnergySource',
                                 name='Source',
                                 base_voltage=bv_11kv,
                                 voltage_magnitude=11000,
                                 voltage_angle=0.0,
                                 location=energy_source_loc)

    # Note that all types extending IdentifiedObject take an mrid. If it is not provided, a UUID will be generated
    # for you. We create a Terminal specifying its phase and connectivity_node. The ConnectivityNode will also
    # receive a reference to the Terminal as part of the constructor.
    es_t1 = Terminal(mrid='es-t1', phases=PhaseCode.ABC, conducting_equipment=energy_source)

    # This is the starting ConnectivityNode in our network
    # At a bare minimum, at least one connectivity node and one Terminal is required for an EnergySource to be
    # connected to the network. Note however that the network will allow you to add disconnected equipment if that
    # suits your use case.
    # NetworkService.connect_by_mrid is a helper method that will create a ConnectivityNode in the NetworkService for
    # the given mRID (in this case, SourceNode), and connect the passed in Terminal to it (es_t1).
    # You should ensure you always call connect_by_mrid or connect_by_terminal, as this keeps the references between
    # the ConnectivityNode and Terminal in sync.
    network.connect_by_mrid(es_t1, "SourceNode")

    # Any ConductingEquipment must have its terminals added to it before being utilised. This can be performed through
    # the constructor via the terminals_ parameter, or added after initialisation like below. The Terminal must also
    # have its conducting_equipment specified for add_terminal() to succeed.
    energy_source.add_terminal(es_t1)
    network.add(es_t1)

    # An EnergySource has an EnergySourcePhase representing each phase it supplies. This is primarily used for tracing,
    # however more attributes will be added at a later date.
    es_phases = [EnergySourcePhase(mrid="esp1", energy_source=energy_source, phase=SinglePhaseKind.A),
                 EnergySourcePhase(mrid="esp2", energy_source=energy_source, phase=SinglePhaseKind.B),
                 EnergySourcePhase(mrid="esp3", energy_source=energy_source, phase=SinglePhaseKind.C)]

    for phase in es_phases:
        energy_source.add_phase(phase)
        network.add(phase)

    network.add(energy_source)

    # Create the PowerTransformer
    # Note BaseVoltage is not used for the PowerTransformer as it has two separate voltages. rated_u must be populated
    # on both ends.
    power_transformer = PowerTransformer(mrid="PowerTransformer", vector_group=VectorGroup.DYN11, name="PowerTransformer")
    delta_pt_end = PowerTransformerEnd(mrid="delta-pt-end", rated_s=800000, rated_u=11000, r=104.789, x=1047.89,
                                       r0=104.789, x0=1047.89, connection_kind=WindingConnection.D,
                                       power_transformer=power_transformer)
    delta_tap_changer = RatioTapChanger(mrid="rtc1", high_step=4, low_step=1, step=2.0, neutral_step=2, normal_step=2,
                                        step_voltage_increment=0.25, transformer_end=delta_pt_end)
    delta_pt_end.ratio_tap_changer = delta_tap_changer

    network.add(delta_tap_changer)
    network.add(delta_pt_end)

    wye_pt_end = PowerTransformerEnd(mrid="wye-pt-end", rated_s=800000, rated_u=416,
                                     connection_kind=WindingConnection.Yn, power_transformer=power_transformer)
    wye_tap_changer = RatioTapChanger(mrid="rtc2", high_step=2, low_step=1, step=2.0, neutral_step=2, normal_step=2,
                                      step_voltage_increment=0.5, transformer_end=wye_pt_end)
    wye_pt_end.ratio_tap_changer = wye_tap_changer

    network.add(wye_tap_changer)
    network.add(wye_pt_end)

    # Terminals are required on the PowerTransformer, corresponding to each PowerTransformerEnd
    # Note that ordering of the Terminals is significant, and must correspond with the ordering of the ends.
    delta_terminal = Terminal(mrid='pt-t1', phases=PhaseCode.ABC, conducting_equipment=power_transformer)
    # Connect the terminal to the source ConnectivityNode
    network.connect_by_mrid(delta_terminal, 'SourceNode')
    power_transformer.add_terminal(delta_terminal)
    network.add(delta_terminal)

    # Set the terminal on the Delta PowerTransformerEnd (This allows us to correlate the Terminal with the voltage)
    delta_pt_end.terminal = delta_terminal

    # The wye Terminal is connected to the Bus 0 ConnectivityNode
    wye_terminal = Terminal(mrid='pt-t2', phases=PhaseCode.ABCN, conducting_equipment=power_transformer)
    # Connect the terminal to the ConnectivityNode
    network.connect_by_mrid(wye_terminal, 'Bus 0')
    power_transformer.add_terminal(wye_terminal)
    network.add(wye_terminal)

    # Set the terminal on the WYE PowerTransformerEnd (This allows us to correlate the Terminal with the voltage)
    wye_pt_end.terminal = wye_terminal

    # Location
    pt_loc = Location(mrid='pt-loc')
    pt_loc.add_point(PositionPoint(149.10941149863936, 35.26964014234307))
    power_transformer.location = pt_loc
    network.add(pt_loc)

    # Add the ends to the network
    power_transformer.add_end(delta_pt_end)
    power_transformer.add_end(wye_pt_end)

    # Add the PowerTransformer to the network
    network.add(power_transformer)

    # Create the Breaker. It requires a new BaseVoltage of 416V
    # Note Breaker constructor defaults all switch states to CLOSED.
    bv_416v = BaseVoltage(mrid="416v", nominal_voltage=416, name="0.416V")
    network.add(bv_416v)

    # Create a location for the Breaker
    breaker_loc = Location(mrid='breaker-loc')
    breaker_loc.add_point(PositionPoint(149.10967971954085, -35.269618243614396))
    network.add(breaker_loc)

    breaker = Breaker(mrid="Breaker", base_voltage=bv_416v, location=breaker_loc)

    t1 = Terminal(mrid='br-t1', phases=PhaseCode.ABCN, conducting_equipment=breaker)
    network.connect_by_mrid(t1, 'Bus 0')
    breaker.add_terminal(t1)
    network.add(t1)

    t2 = Terminal(mrid='br-t2', phases=PhaseCode.ABCN, conducting_equipment=breaker)
    network.connect_by_mrid(t2, 'Bus 1')
    breaker.add_terminal(t2)
    network.add(t2)

    # Add the Breaker to the network
    network.add(breaker)

    # Create first AcLineSegment with a PerLengthSequenceImpedance and Location with two position_points
    # The properties of the conductor are contained in a PerLengthSequenceImpedance. All amounts are per metre.
    plsi_1 = PerLengthSequenceImpedance(mrid="4c_70", r=0.446, x=0.071, r0=1.505, x0=0.083)
    network.add(plsi_1)

    # A line typically has two longlats representing each terminal point. Note these must be added in order and
    # correspond to the matching Terminal.
    acls1_loc = Location(mrid="acls1-loc")
    acls1_loc.add_point(PositionPoint(149.10967971954085, -35.269618243614396))
    acls1_loc.add_point(PositionPoint(149.11003377113082, -35.27061681962318))
    network.add(acls1_loc)

    acls1 = AcLineSegment(mrid="acls1",
                          base_voltage=bv_416v,
                          length=0.03171938,
                          per_length_sequence_impedance=plsi_1,
                          location=acls1_loc)

    # Create and add the Terminal's for this ACLS
    acls1_t1 = Terminal(mrid='acls1-t1', phases=PhaseCode.ABCN, conducting_equipment=acls1)
    network.connect_by_mrid(acls1_t1, "Bus 1")
    acls1.add_terminal(acls1_t1)
    network.add(acls1_t1)

    acls1_t2 = Terminal(mrid='acls1-t2', phases=PhaseCode.ABCN, conducting_equipment=acls1)
    network.connect_by_mrid(acls1_t2, "Bus 25")
    acls1.add_terminal(acls1_t2)
    network.add(acls1_t2)

    # Add the ACLS to the network after adding all its dependencies
    network.add(acls1)

    # Create the rest of the lines. These are all quite similar to above.
    create_lines(network, bv_416v, "Bus 25", plsi_1)

    # Create first EnergyConsumer on 230V with a single phase
    bv_230v = BaseVoltage(mrid="230v", nominal_voltage=230, name="0.23kV")
    network.add(bv_230v)

    # Each EnergyConsumer has a list of phases it is connected on. These are optional and only used to
    # specify additional properties per phase when they are known.
    ecp = [EnergyConsumerPhase(mrid='ecp1', phase=SinglePhaseKind.A, p_fixed=800.0, q_fixed=200.0)]
    network.add(ecp[0])

    # Create the EnergyConsumer's Location
    ec1_loc = Location(mrid="ec1-loc")
    ec1_loc.add_point(PositionPoint(149.11037709388472, -35.269758395375725))
    network.add(ec1_loc)

    energy_consumer1 = EnergyConsumer(mrid="EnergyConsumer1",
                                      p=1000,
                                      q=334.27413609633844,
                                      name="Load 1",
                                      phase_connection=PhaseShuntConnectionKind.Y,
                                      energy_consumer_phases=ecp,
                                      base_voltage=bv_230v,
                                      location=ec1_loc)

    # Create and add the Terminal, Note that the ConnectivityNode will be retrieved from the network (Bus 34 was created in create_lines())
    ec1_t1 = Terminal(mrid='ec1-t1', phases=PhaseCode.A, conducting_equipment=energy_consumer1)
    network.connect_by_mrid(ec1_t1, "Bus 34")
    energy_consumer1.add_terminal(ec1_t1)
    network.add(ec1_t1)

    # Add the energy consumer to the network
    network.add(energy_consumer1)

    # Create PV EnergySource with a single phase and a location
    esp = [EnergySourcePhase(mrid='esp4', phase=SinglePhaseKind.A)]
    network.add(esp[0])

    es2_loc = Location(mrid="es2-loc")
    es2_loc.add_point(PositionPoint(149.1106238571141, -35.269881027967976))
    network.add(es2_loc)

    energy_source_pv = EnergySource(mrid="PV-DG",
                                    name='PV Distributed Generator',
                                    base_voltage=bv_230v,
                                    voltage_magnitude=416,
                                    voltage_angle=9.0,
                                    energy_source_phases=esp,
                                    location=es2_loc)
    es2_t1 = Terminal(mrid='es2-t1', phases=PhaseCode.A, conducting_equipment=energy_source_pv)
    network.connect_by_mrid(es2_t1, "Bus 34")
    energy_source_pv.add_terminal(es2_t1)
    network.add(es2_t1)

    # Add the EnergySource to the network.
    network.add(energy_source_pv)

    # Create second EnergyConsumer with a single phase of B and a Location.
    ecp = [EnergyConsumerPhase(mrid="ecp2", phase=SinglePhaseKind.B, p_fixed=800.0, q_fixed=200.0)]
    network.add(ecp[0])

    ec2_loc = Location(mrid="ec2-loc")
    ec2_loc.add_point(PositionPoint(149.1108598915074, -35.271930716670994))
    network.add(ec2_loc)

    energy_consumer2 = EnergyConsumer(mrid="EnergyConsumer2",
                                      p=1000,
                                      q=334.27413609633844,
                                      name="Load 2",
                                      phase_connection=PhaseShuntConnectionKind.Y,
                                      energy_consumer_phases=ecp,
                                      base_voltage=bv_230v,
                                      location=ec2_loc)

    # Create and connect the Terminal.
    ec2_t1 = Terminal(mrid='ec2-t1', phases=PhaseCode.B, conducting_equipment=energy_consumer2)
    network.connect_by_mrid(ec2_t1, "Bus 47")
    energy_consumer2.add_terminal(ec2_t1)
    network.add(ec2_t1)

    # Add the EnergyConsumer to the network
    network.add(energy_consumer2)

    # Create third EnergyConsumer with a single phase A and a Location.
    ecp = [EnergyConsumerPhase(mrid="ecp3", phase=SinglePhaseKind.A, p_fixed=800.0, q_fixed=200.0)]
    network.add(ecp[0])

    ec3_loc = Location(mrid="ec3-loc", position_points=[PositionPoint(149.11048438224535
                                                                      , -35.27024892463094)])
    network.add(ec3_loc)

    energy_consumer3 = EnergyConsumer(mrid="EnergyConsumer3",
                                      p=1000,
                                      q=334.27413609633844,
                                      name="Load3",
                                      phase_connection=PhaseShuntConnectionKind.Y,
                                      energy_consumer_phases=ecp,
                                      base_voltage=bv_230v,
                                      location=ec3_loc)
    ec3_t1 = Terminal(mrid='ec3-t1', phases=PhaseCode.A, conducting_equipment=energy_consumer3)
    network.connect_by_mrid(ec3_t1, "Bus 70")
    energy_consumer3.add_terminal(ec3_t1)
    network.add(ec3_t1)

    # Add the last energy consumer to the network
    network.add(energy_consumer3)

    # Our network is complete and connected. Next we must send it.
    return network


def create_lines(network, bv_416v, connectivity_node_mrid, plsi_1):
    """
    Create lines (in depth-first order, starting from top). Note re-use of PerLengthSequenceImpedances.
    """
    # ACLS2
    acls2_loc = Location(mrid="acls2-loc")
    acls2_loc.add_point(PositionPoint(149.11003377113082, -35.27061681962318))
    acls2_loc.add_point(PositionPoint(149.11010887298323, -35.2700474576177))
    network.add(acls2_loc)

    plsi_2 = PerLengthSequenceImpedance(mrid="2c_16", r=1.150, x=0.088, r0=1.200, x0=0.088)
    network.add(plsi_2)

    acls2 = AcLineSegment(mrid="acls2",
                          base_voltage=bv_416v,
                          length=0.00675,
                          per_length_sequence_impedance=plsi_2,
                          location=acls2_loc)

    acls2_t1 = Terminal(mrid='acls2-t1', phases=PhaseCode.ABCN, conducting_equipment=acls2)
    network.connect_by_mrid(acls2_t1, connectivity_node_mrid)
    acls2.add_terminal(acls2_t1)
    network.add(acls2_t1)

    acls2_t2 = Terminal(mrid='acls2-t2', phases=PhaseCode.ABCN, conducting_equipment=acls2)
    network.connect_by_mrid(acls2_t2, 'Bus 27')
    acls2.add_terminal(acls2_t2)
    network.add(acls2_t2)
    network.add(acls2)

    # ACLS3
    acls3_loc = Location(mrid="acls3-loc")
    acls3_loc.add_point(PositionPoint(149.11010887298323
                                      , -35.2700474576177))
    acls3_loc.add_point(PositionPoint(149.11038782272078, -35.269889787431744))
    network.add(acls3_loc)

    acls3 = AcLineSegment(mrid="acls3",
                          base_voltage=bv_416v,
                          length=0.0053248,
                          per_length_sequence_impedance=plsi_2,
                          location=acls3_loc)

    acls3_t1 = Terminal(mrid='acls3-t1', phases=PhaseCode.ABCN, conducting_equipment=acls3)
    network.connect_by_mrid(acls3_t1, 'Bus 27')
    acls3.add_terminal(acls3_t1)
    network.add(acls3_t1)

    acls3_t2 = Terminal(mrid='acls3-t2', phases=PhaseCode.ABCN, conducting_equipment=acls3)
    network.connect_by_mrid(acls3_t2, 'Bus 34')
    acls3.add_terminal(acls3_t2)
    network.add(acls3_t2)

    network.add(acls3)

    # ACLS4
    acls4_loc = Location(mrid="acls4-loc")
    acls4_loc.add_point(PositionPoint(149.11010887298323, -35.2700474576177))
    acls4_loc.add_point(PositionPoint(149.11048438224535, -35.27024892463094))
    network.add(acls4_loc)

    acls4 = AcLineSegment(mrid="acls4",
                          base_voltage=bv_416v,
                          length=0.0756,
                          per_length_sequence_impedance=plsi_2,
                          location=acls4_loc)

    acls4_t1 = Terminal(mrid='acls4-t1', phases=PhaseCode.ABCN, conducting_equipment=acls4)
    network.connect_by_mrid(acls4_t1, 'Bus 27')
    acls4.add_terminal(acls4_t1)
    network.add(acls4_t1)

    acls4_t2 = Terminal(mrid='acls4-t2', phases=PhaseCode.ABCN, conducting_equipment=acls4)
    network.connect_by_mrid(acls4_t2, 'Bus 70')
    acls4.add_terminal(acls4_t2)
    network.add(acls4_t2)
    network.add(acls4)

    # ACLS5
    acls5_loc = Location(mrid="acls5-loc")
    acls5_loc.add_point(PositionPoint(149.11003377113082, -35.27061681962318))
    acls5_loc.add_point(PositionPoint(149.110248347852, -35.27128253012409))
    network.add(acls5_loc)

    acls5 = AcLineSegment(mrid="acls5",
                          base_voltage=bv_416v,
                          length=0.0255,
                          per_length_sequence_impedance=plsi_1,
                          location=acls5_loc)

    acls5_t1 = Terminal(mrid='acls5-t1', phases=PhaseCode.ABCN, conducting_equipment=acls5)
    network.connect_by_mrid(acls5_t1, connectivity_node_mrid)
    acls5.add_terminal(acls5_t1)
    network.add(acls5_t1)

    acls5_t2 = Terminal(mrid='acls5-t2', phases=PhaseCode.ABCN, conducting_equipment=acls5)
    network.connect_by_mrid(acls5_t2, 'Bus 32')
    acls5.add_terminal(acls5_t2)
    network.add(acls5_t2)
    network.add(acls5)

    # ACLS6
    acls6_loc = Location(mrid="acls6-loc")
    acls6_loc.add_point(PositionPoint(149.110248347852, -35.27128253012409))
    acls6_loc.add_point(PositionPoint(149.11031272086836, -35.27192195742791))
    network.add(acls6_loc)

    acls6 = AcLineSegment(mrid="acls6",
                          base_voltage=bv_416v,
                          length=0.0057,
                          per_length_sequence_impedance=plsi_2,
                          location=acls6_loc)

    acls6_t1 = Terminal(mrid='acls6-t1', phases=PhaseCode.ABCN, conducting_equipment=acls6)
    network.connect_by_mrid(acls6_t1, 'Bus 32')
    acls6.add_terminal(acls6_t1)
    network.add(acls6_t1)

    acls6_t2 = Terminal(mrid='acls6-t2', phases=PhaseCode.ABCN, conducting_equipment=acls6)
    network.connect_by_mrid(acls6_t2, 'Bus 36')
    acls6.add_terminal(acls6_t2)
    network.add(acls6_t2)
    network.add(acls6)

    # ACLS7
    acls7_loc = Location(mrid="acls7-loc")
    acls7_loc.add_point(PositionPoint(149.11031272086836, -35.27192195742791))
    acls7_loc.add_point(PositionPoint(149.1108598915074, -35.271930716670994))
    network.add(acls7_loc)

    acls7 = AcLineSegment(mrid="acls7",
                          base_voltage=bv_416v,
                          length=0.01265,
                          per_length_sequence_impedance=plsi_2,
                          location=acls7_loc)
    acls7_t1 = Terminal(mrid='acls7-t1', phases=PhaseCode.ABCN, conducting_equipment=acls7)
    network.connect_by_mrid(acls7_t1, 'Bus 36')
    acls7.add_terminal(acls7_t1)
    network.add(acls7_t1)

    acls7_t2 = Terminal(mrid='acls7-t2', phases=PhaseCode.ABCN, conducting_equipment=acls7)
    network.connect_by_mrid(acls7_t2, 'Bus 47')
    acls7.add_terminal(acls7_t2)
    network.add(acls7_t2)
    network.add(acls7)


async def main():
    parser = argparse.ArgumentParser(description="Zepben cimbend demo for a basic LV test feeder")
    parser.add_argument('server', help='Host and port of grpc server', metavar="host:port", nargs="?",
                        default="localhost")
    parser.add_argument('--rpc-port', help="The gRPC port for the server", default="50051")
    parser.add_argument('--conf-address', help="The address to retrieve auth configuration from",
                        default="http://localhost/auth")
    parser.add_argument('--client-id', help='Auth0 M2M client id', default="")
    parser.add_argument('--client-secret', help='Auth0 M2M client secret', default="")
    parser.add_argument('--ca', help='CA trust chain', default="")
    parser.add_argument('--cert', help='Signed certificate for your client', default="")
    parser.add_argument('--key', help='Private key for signed cert', default="")
    args = parser.parse_args()
    ca = cert = key = client_id = client_secret = None
    if not args.client_id or not args.client_secret or not args.ca or not args.cert or not args.key:
        logger.warning(
            f"Using an insecure connection as at least one of (--ca, --token, --cert, --key) was not provided.")
    else:
        with open(args.key, 'rb') as f:
            key = f.read()
        with open(args.ca, 'rb') as f:
            ca = f.read()
        with open(args.cert, 'rb') as f:
            cert = f.read()
        client_secret = args.client_secret
        client_id = args.client_id

    # Creates a Network
    net = create_network()
    diagram = create_diagram()

    # Connect to a local postbox instance using credentials if provided.
    async with connect_async(host=args.server, rpc_port=args.rpc_port, conf_address=args.conf_address,
                             client_id=client_id, client_secret=client_secret, pkey=key, cert=cert, ca=ca) as channel:
        # Send the network to the postbox instance.
        client = ProducerClient(channel=channel)
        res = await client.send([net, diagram])

        # joJoTODO: Examples of querying EWB


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
