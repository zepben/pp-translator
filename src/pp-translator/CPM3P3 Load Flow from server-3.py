import argparse
import asyncio

import geopandas
import numpy as np
import pandapower as pp
import pandas as pd
import zepben.evolve
from zepben.evolve import Equipment
from zepben.evolve import NetworkService, Feeder, EnergySource, EnergySourcePhase, ConnectivityNode, \
    ConductingEquipment, BaseVoltage, AcLineSegment, EnergyConsumer, \
    Disconnector, Breaker, PowerTransformer, Fuse, Recloser, EnergyConsumerPhase
from zepben.evolve import connect_async
from zepben.evolve import connect_async, NetworkConsumerClient

def conn_to_junction(Connectivity_Point):
    # TODO: Solve problem of connectivity when there are fuses with more than 2 terminals
    Node = Connectivity_Point
    for Terminal in Connectivity_Point._terminals:
        if (type(Terminal._conducting_equipment) == zepben.evolve.model.cim.iec61970.base.wires.connectors.Junction):
            Node = Terminal._conducting_equipment
    return Node


def Recorrer(Origin, Visited):
    Voltage = None
    for Tramo in Origin._terminals:
        if Tramo._conducting_equipment.base_voltage != None:
            Voltage = Tramo._conducting_equipment.base_voltage.nominal_voltage
            break
        if Tramo._conducting_equipment not in Visited and Tramo._conducting_equipment.base_voltage == None:
            Visited.append(Tramo._conducting_equipment)
            for Terminal in Tramo._conducting_equipment._terminals:
                if Terminal.connectivity_node != Origin:
                    Recorrer(Terminal.connectivity_node, Visited)
    return Voltage


def search_voltage(Connectivity_Point):
    # TODO: Solve problem of connectivity when there are fuses with more than 2 terminals
    Node = Connectivity_Point
    Voltage = None
    for Terminal in Connectivity_Point._terminals:

        if Voltage == None:
            if Terminal._conducting_equipment.base_voltage != None:
                Voltage = Terminal._conducting_equipment.base_voltage.nominal_voltage
            # for Element in Terminal._conducting_equipment:

            else:
                Voltage = None
    return Voltage


def create_bus(service, net):
    # TODO:Function to obtain BaseVoltage
    MV = 11000 / 1000
    LV = 11000 / 1000
    data = {'name': [], 'angle': [], 'vn_kv': [], 'TopologicalNode_mRID': [], 'X': [], 'Y': [],
            'ConductingEquipment_mRID': []}
    df = pd.DataFrame(data)

    for Term in service.objects(ConnectivityNode):
        if search_voltage(Term) != None:
            for Terminal in Term._terminals:
                new_row = {'TopologicalNode_mRID': Term.mrid, 'vn_kv': search_voltage(Term), 'angle': 0,
                           'name': conn_to_junction(Term).mrid,
                           'ConductingEquipment_mRID': Terminal._conducting_equipment.mrid}
                df = df.append(new_row, ignore_index=True)
    df2 = df.drop(['TopologicalNode_mRID', 'angle', 'ConductingEquipment_mRID'], axis=1)
    df2 = pd.DataFrame.drop_duplicates(df2)
    for idx in df2.index:
        pp.create_bus(net, name=df2.at[idx, "name"], vn_kv=df2.at[idx, "vn_kv"])
    # print(len(df2))
    df2 = df.drop(['X', 'Y'], axis=1)
    return df


def print_feeder_eq(service):
    datavolt = {'name': [], 'type_element': [], 'basevol': []}
    df = pd.DataFrame(datavolt)
    for equip in service.objects(ConductingEquipment):
        if equip.get_base_voltage() != None:
            print(equip.mrid, type(equip).__name__, equip.base_voltage.mrid, equip.base_voltage.nominal_voltage)
            new_row = {'name': equip.mrid, 'type_element': type(equip).__name__, 'basevol': equip.base_voltage.mrid}
            df = df.append(new_row, ignore_index=True)
    return df


def create_disconnector(service, net, a):
    for Switch in service.objects(Disconnector):
        if len(Switch._terminals) == 2 and Switch.base_voltage != None:
            bus = get_index(conn_to_junction(Switch._terminals[0].connectivity_node).mrid, a)
            element = get_index(conn_to_junction(Switch._terminals[1].connectivity_node).mrid, a)
            et = 'b'
            type = "CB"
            pp.create_switch(net, name=Switch.mrid, bus=bus, element=element, et=et, type=type)
            # print("The Switch:"+ Switch.mrid,"|",Switch.in_service)
            net.bus_geodata.at[bus, "x"] = (Switch.location._position_points[0].x_position)
            net.bus_geodata.at[bus, "y"] = (Switch.location._position_points[0].y_position)
            net.bus_geodata.at[element, "x"] = (Switch.location._position_points[0].x_position)
            net.bus_geodata.at[element, "y"] = (Switch.location._position_points[0].y_position)


def create_fuse(service, net, a):
    for Switch in service.objects(Fuse):
        bus = get_index(conn_to_junction(Switch._terminals[0].connectivity_node).mrid, a)
        element = get_index(conn_to_junction(Switch._terminals[1].connectivity_node).mrid, a)
        et = 'b'
        type = "CB"
        pp.create_switch(net, name=Switch.mrid, bus=bus, element=element, et=et, type=type)
        # print("The fuses:"+ Switch.mrid,"|",Switch.in_service)
        net.bus_geodata.at[bus, "x"] = (Switch.location._position_points[0].x_position)
        net.bus_geodata.at[bus, "y"] = (Switch.location._position_points[0].y_position)
        net.bus_geodata.at[element, "x"] = (Switch.location._position_points[0].x_position)
        net.bus_geodata.at[element, "y"] = (Switch.location._position_points[0].y_position)


def create_recloser(service, net, a):
    for Switch in service.objects(Recloser):
        if len(Switch._terminals) == 2:
            bus = get_index(conn_to_junction(Switch._terminals[0].connectivity_node).mrid, a)
            element = get_index(conn_to_junction(Switch._terminals[1].connectivity_node).mrid, a)
            et = 'b'
            type = "CB"
            pp.create_switch(net, name=Switch.mrid, bus=bus, element=element, et=et, type=type)

            net.bus_geodata.at[bus, "x"] = (Switch.location._position_points[0].x_position)
            net.bus_geodata.at[bus, "y"] = (Switch.location._position_points[0].y_position)
            net.bus_geodata.at[element, "y"] = (Switch.location._position_points[0].y_position)
            net.bus_geodata.at[element, "y"] = (Switch.location._position_points[0].y_position)


def get_index(Nombre_Nodo, Nodos):
    dfb = int(Nodos[Nodos['name'] == Nombre_Nodo].index[0])
    return dfb


def get_head_feeder(service, a):
    for Feederp in service.objects(Feeder):
        Node_head = get_index(conn_to_junction(Feederp.normal_head_terminal.connectivity_node).mrid, a)
        return (Node_head)


def get_base_voltage(service):
    for Base in service.objects(BaseVoltage):
        print("The BaseVoltage:" + Base.mrid)


def get_energy_source_phases(service):
    for PhaseGen in service.objects(EnergySourcePhase):
        print("The EnergySourcePhases", PhaseGen.mrid, PhaseGen.name)


def get_energy_consumer_phases(service):
    for PhaseLoads in service.objects(EnergyConsumerPhase):
        print("The EnergyConsumerPhases", PhaseLoads.mrid, PhaseLoads.name)


def create_switch(service, net, a):
    for Switch in service.objects(Breaker):
        bus = get_index(conn_to_junction(Switch._terminals[0].connectivity_node).mrid, a)
        element = get_index(conn_to_junction(Switch._terminals[1].connectivity_node).mrid, a)
        et = 'b'
        type = "CB"
        pp.create_switch(net, name=Switch.mrid, bus=bus, element=element, et=et, type=type)
        # print("The Switch in Pandapower are:"+ Switch.mrid,"|",Switch.in_service)
        net.bus_geodata.at[bus, "x"] = (Switch.location._position_points[0].x_position)
        net.bus_geodata.at[bus, "y"] = (Switch.location._position_points[0].y_position)
        net.bus_geodata.at[element, "x"] = (Switch.location._position_points[0].x_position)
        net.bus_geodata.at[element, "y"] = (Switch.location._position_points[0].y_position)


def create_transformers(service, net, a):
    # TODO:Validate power transformers parameters required to load flow
    for Transformer in service.objects(PowerTransformer):
        print("El transssformer es", type(Transformer))
        if (len(Transformer._terminals) == 2):

            try:
                # if Transformer._terminals[0]._conducting_equipment.base_voltage.nominal_voltage > Transformer._terminals[1]._conducting_equipment.base_voltage.nominal_voltage:
                NODE1 = get_index(conn_to_junction(Transformer._terminals[0].connectivity_node).mrid, a)
                NODE2 = get_index(conn_to_junction(Transformer._terminals[1].connectivity_node).mrid, a)
                if net.bus.at[NODE1, "vn_kv"] > net.bus.at[NODE2, "vn_kv"]:
                    hv_bus = NODE1
                    lv_bus = NODE2
                else:
                    hv_bus = NODE2
                    lv_bus = NODE1
                sn_mva = Transformer._power_transformer_ends[0].rated_s / 1000000
                vn_hv_kv = 11
                vn_lv_kv = 0.415
                vk_percent = 4
                vkr_percent = 0.4
                pfe_kw = 0.03
                i0_percent = 1
                shift_degree = 0
                pp.create_transformer_from_parameters(net, name=Transformer.mrid, hv_bus=hv_bus, lv_bus=lv_bus,
                                                      sn_mva=sn_mva,
                                                      vn_hv_kv=vn_hv_kv, vn_lv_kv=vn_lv_kv, vkr_percent=vkr_percent,
                                                      vk_percent=vk_percent, pfe_kw=pfe_kw, i0_percent=i0_percent,
                                                      shift_degree=shift_degree, vector_group="Dyn")
                print("The Power transformers are:" + Transformer.mrid, "|", hv_bus, "|", lv_bus, "|", sn_mva)
                net.bus_geodata.at[hv_bus, "x"] = (Transformer.location._position_points[0].x_position)
                net.bus_geodata.at[hv_bus, "y"] = (Transformer.location._position_points[0].y_position)
                # net.bus_geodata.at[lv_bus,"x"]=(Transformer.location._position_points[1].x_position)
                # net.bus_geodata.at[lv_bus,"y"]=(Transformer.location._position_points[1].y_position)
            except:
                print("No hay trafo")


def create_generators(service, net, a, Node_head, df2):
    # TODO:Validate generators parameters required to load flow (Actually these values are constants)
    for Ext in service.objects(EnergySource):
        bus = get_index(conn_to_junction(Ext._terminals[0].connectivity_node).mrid, a)
        if bus == Node_head:
            p_mw = Ext.p_max + 1
            pp.create_ext_grid(net, bus=bus, name=Ext.mrid, va_degree=0, s_sc_max_mva=1000, s_sc_min_mva=800,
                               rx_max=0.1, r0x0_max=1, x0x_max=1, r0x0_min=0.1, x0x_min=1)
            print("Las redes externas son:" + Ext.mrid, "|", bus, Ext.voltage_magnitude, p_mw)
    if net.ext_grid.empty == True:
        # pp.create_ext_grid(net,name="External_Grid",bus=Node_head,p_mw=10,va_degree=0,s_sc_max_mva=1000,s_sc_min_mva=800,rx_max=0.1)
        pp.create_ext_grid(net, name="External_Grid", bus=Node_head, va_degree=0, s_sc_max_mva=1000,
                           s_sc_min_mva=800, rx_max=0.1)

    for eq in service.objects(Equipment):
        for up in list(eq.usage_points):
            for mt in list(up.end_devices):
                for idx in df2.index:
                    # print(int(df.at[idx,"premnum"]),"/",mt.service_location.name)
                    if int(df2.at[idx, "premnum"]) == int(mt.service_location.name):
                        # Fases = eq._terminals[0].phases.
                        Fases = "ABC"
                        bus = get_index(conn_to_junction(eq._terminals[0].connectivity_node).mrid, a)
                        p_mw = df2.at[idx, "Gen"] / 1000
                        q_mvar = 0
                        # sn_mva = Generators.p_max
                        # angle = Generators.voltage_angle
                        sn_mva = 1
                        angle = 0
                        slack = False
                        type = "PV"
                        if bus != Node_head:
                            if Fases == "A":
                                pp.create_asymmetric_sgen(net, name="Gen" + eq.mrid, bus=bus, p_a_mw=p_mw,
                                                          q_a_mvar=q_mvar, sn_mva=sn_mva, type=type)
                                # pp.create_asymmetric_sgen(net, name="Gen"+eq.mrid, bus=bus, p_a_mw=p_mw, q_a_mvar=q_mvar,sn_mva=sn_mva, type=type)
                            elif Fases == "B":
                                pp.create_asymmetric_sgen(net, name="Gen" + eq.mrid, bus=bus, p_b_mw=p_mw,
                                                          q_b_mvar=q_mvar, sn_mva=sn_mva, type=type)
                            elif Fases == "C":
                                pp.create_asymmetric_sgen(net, name="Gen" + eq.mrid, bus=bus, p_c_mw=p_mw,
                                                          q_c_mvar=q_mvar, sn_mva=sn_mva, type=type)
                            elif Fases == "ABC" or Fases == "ABCN":
                                # pp.create_gen(net, name=Generators.mrid, bus=bus, p_mw=p_mw)
                                pp.create_gen(net, name="Gen" + eq.mrid, bus=bus, p_mw=p_mw)
                            # print("2")
                            # print("Los generadores son:" + Generators.mrid, "|", bus, Generators.voltage_magnitude, sn_mva, angle)
                            # print("Los generadores son:" + "Gen"+eq.mrid, "|", bus, Generators.voltage_magnitude, sn_mva, angle)
                        break


'''def create_generators(service, net, a, Node_head,df2):
    # TODO:Validate generators parameters required to load flow (Actually these values are constants)
    for Ext in service.objects(EnergySource):
        bus = get_index(conn_to_junction(Ext._terminals[0].connectivity_node).mrid, a)
        if bus == Node_head:
            p_mw = Ext.p_max + 1
            pp.create_ext_grid(net, bus=bus, name=Ext.mrid, va_degree=0, s_sc_max_mva=1000, s_sc_min_mva=800,
                               rx_max=0.1, r0x0_max=1, x0x_max=1, r0x0_min=0.1, x0x_min=1)
            print("Las redes externas son:" + Ext.mrid, "|", bus, Ext.voltage_magnitude, p_mw)
    if net.ext_grid.empty == True:
        # pp.create_ext_grid(net,name="External_Grid",bus=Node_head,p_mw=10,va_degree=0,s_sc_max_mva=1000,s_sc_min_mva=800,rx_max=0.1)
        pp.create_ext_grid(net, name="External_Grid", bus=Node_head, va_degree=0, s_sc_max_mva=1000,
                           s_sc_min_mva=800, rx_max=0.1)

    for Generators in service.objects(EnergySource):
        print("El generador es" + Generators.mrid)
        Fases = Generators._terminals[0].phases.name
        bus = get_index(conn_to_junction(Generators._terminals[0].connectivity_node).mrid, a)
        try:
            p_mw = Generators.active_power + 0.002
        except:
            p_mw = 0.002
        try:
            q_mvar = Generators.reactive_power + 0.001
        except:
            q_mvar = 0.0001
        sn_mva = Generators.p_max
        angle = Generators.voltage_angle
        slack = False
        type = "PV"

        if bus != Node_head:
            if Fases == "A":
                pp.create_asymmetric_sgen(net, name=Generators.mrid, bus=bus, p_a_mw=p_mw, q_a_mvar=q_mvar,
                                          sn_mva=sn_mva, type=type)
            elif Fases == "B":
                pp.create_asymmetric_sgen(net, name=Generators.mrid, bus=bus, p_b_mw=p_mw, q_b_mvar=q_mvar,
                                          sn_mva=sn_mva, type=type)
            elif Fases == "C":
                pp.create_asymmetric_sgen(net, name=Generators.mrid, bus=bus, p_c_mw=p_mw, q_c_mvar=q_mvar,
                                          sn_mva=sn_mva, type=type)
            elif Fases == "ABC" or Fases == "ABCN":
                pp.create_gen(net, name=Generators.mrid, bus=bus, p_mw=p_mw)
            # print("2")
            print("Los generadores son:" + Generators.mrid, "|", bus, Generators.voltage_magnitude, sn_mva, angle)
    # print(net.gen)
'''


def create_load(service, net, a, df):
    # TODO:Validate value loads.
    # This case is when the phases are defined.
    for Load in service.objects(EnergyConsumer):
        Fases = Load._terminals[0].phases.name
        # Fases="ABC"
        bus = get_index(conn_to_junction(Load._terminals[0].connectivity_node).mrid, a)

        for up in list(Load.usage_points):
            for mt in list(up.end_devices):
                # print(f"{Load.mrid}/{Load.name}/{mt.service_location.name}ESTE INICIO")
                # print(df)
                for idx in df.index:
                    # print(int(df.at[idx,"premnum"]),"/",mt.service_location.name)
                    if int(df.at[idx, "premnum"]) == int(mt.service_location.name):
                        p_mw = df.at[idx, "Carga"] / 1000
                        # print(f"{Load.mrid}/{Load.name}/{mt.service_location.name}/{Fases}/ESTE FIN")
                        # Load=5

                        # print(Load)
                        q_mvar = 0
                        # p_mw = (Load.p / 1000000) + 0.001
                        # q_mvar = (Load.q / 1000000) + 0.0003

                        if Fases == "A":
                            pp.create_asymmetric_load(net, name=Load.mrid, bus=bus, p_a_mw=p_mw, q_a_mvar=q_mvar,
                                                      type='wye')
                        elif Fases == "B":
                            pp.create_asymmetric_load(net, name=Load.mrid, bus=bus, p_b_mw=p_mw, q_b_mvar=q_mvar,
                                                      type='wye')
                        elif Fases == "C":
                            pp.create_asymmetric_load(net, name=Load.mrid, bus=bus, p_c_mw=p_mw, q_c_mvar=q_mvar,
                                                      type='wye')
                        elif Fases == "ABC" or Fases == "ABCN" or Fases == "XN":
                            pp.create_load(net, name=Load.mrid, bus=bus, p_mw=p_mw, q_mvar=q_mvar)
                        break


def create_load_with_phases(service, net, a):
    # This function create randomly the phases when these are not defined
    c = 0
    for Load in service.objects(EnergyConsumer):
        Fases = Load._terminals[0].phases.name
        bus = get_index(conn_to_junction(Load._terminals[0].connectivity_node).mrid, a)
        p_mw = (Load.p / 1000000) + 0.001
        q_mvar = (Load.q / 1000000) + 0.0003
        module = c % 3
        if module == 0:
            pp.create_asymmetric_load(net, name=Load.mrid, bus=bus, p_a_mw=p_mw, q_a_mvar=q_mvar, type='wye')
        elif module == 1:
            pp.create_asymmetric_load(net, name=Load.mrid, bus=bus, p_b_mw=p_mw, q_b_mvar=q_mvar, type='wye')
        elif module == 2:
            pp.create_asymmetric_load(net, name=Load.mrid, bus=bus, p_c_mw=p_mw, q_c_mvar=q_mvar, type='wye')
        c += 1


def create_lines(service, net, a):
    # TODO:Validate parameters of the lines.
    longitud = 0
    for Equip in service.objects(AcLineSegment):
        if Equip.base_voltage != None:
            from_bus = get_index(conn_to_junction(Equip._terminals[0].connectivity_node).mrid, a)
            to_bus = get_index(conn_to_junction(Equip._terminals[1].connectivity_node).mrid, a)
            length_km = Equip.length / 1000
            if length_km < 0.01:
                length_km = 0.01
            longitud = longitud + length_km

            r_ohm_per_km = (Equip.per_length_sequence_impedance.r) + 0.001
            x_ohm_per_km = (Equip.per_length_sequence_impedance.x) + 0.001
            c_nf_per_km = (Equip.per_length_sequence_impedance.bch) + 0.001
            # print(Equip.per_length_sequence_impedance.r,"|", length_km,"|",Equip.per_length_sequence_impedance.r*length_km,"|",r_ohm_per_km)
            r0_ohm_per_km = (Equip.per_length_sequence_impedance.r0) + 0.001
            x0_ohm_per_km = (Equip.per_length_sequence_impedance.x0) + 0.001
            c0_nf_per_km = (Equip.per_length_sequence_impedance.x0) + 0.001
            max_i_ka = 0.4
            pp.create_line_from_parameters(net, name=Equip.mrid, from_bus=from_bus, to_bus=to_bus, length_km=length_km,
                                           c_nf_per_km=c_nf_per_km, max_i_ka=max_i_ka, r_ohm_per_km=r_ohm_per_km,
                                           x_ohm_per_km=x_ohm_per_km)
            # print(Equip.location._position_points[0].x_position)
            net.bus_geodata.at[from_bus, "x"] = (Equip.location._position_points[0].x_position)
            net.bus_geodata.at[from_bus, "y"] = (Equip.location._position_points[0].y_position)
            net.bus_geodata.at[to_bus, "x"] = (Equip.location._position_points[1].x_position)
            net.bus_geodata.at[to_bus, "y"] = (Equip.location._position_points[1].y_position)
    print("la longitud es", longitud)


def name_busbar(res_voltage, a):
    output = pd.merge(res_voltage.drop(['p_mw', 'q_mvar'], axis=1), a.drop(['type', 'zone', 'in_service'], axis=1),
                      left_index=True, right_index=True)
    return output


def result_busbar(name_bus, b, term_results):
    # output = pd.merge(res_voltage.drop(['p_mw','q_mvar'],axis=1), a.drop(['type','zone','in_service'],axis=1),left_index=True, right_index=True)
    output = pd.merge(name_bus, b, left_index=True, right_index=True)
    output = output[output.vm_pu.notnull()]
    for idx in output.index:
        output.at[idx, 'vn_kv'] = int(output.at[idx, 'vn_kv'] * output.at[idx, 'vm_pu'] * 1000)
    output = pd.merge(term_results.drop(['angle', 'vn_kv', 'X', 'Y'], axis=1), output.drop(['vm_pu', 'coords'], axis=1),
                      "inner", left_on="name", right_on="name")
    return output


def result_lines(res_line, line, name_bus, term_results):
    output = pd.merge(res_line.drop(
        ['loading_percent', 'pl_mw', 'ql_mvar', 'i_from_ka', 'i_to_ka', 'i_ka', 'vm_from_pu', 'va_from_degree',
         'vm_to_pu', 'va_to_degree'], axis=1), line.drop(
        ['length_km', 'r_ohm_per_km', 'x_ohm_per_km', 'c_nf_per_km', 'g_us_per_km', 'max_i_ka', 'df', 'type',
         'parallel', 'in_service', 'std_type'], axis=1), left_index=True, right_index=True)
    output = pd.merge(output, name_bus.drop(['vm_pu', 'va_degree', 'vn_kv'], axis=1), "inner", left_on="from_bus",
                      right_index=True, suffixes=("_LINE", "_NODE1"))
    output = pd.merge(output, name_bus.drop(['vm_pu', 'va_degree', 'vn_kv'], axis=1), "inner", left_on="to_bus",
                      right_index=True, suffixes=("_NODE1", "_NODE2"))
    term_results = pd.merge(term_results, output.drop(['p_to_mw', 'q_to_mvar', 'from_bus', 'to_bus', 'name'], axis=1),
                            left_on=['name', 'ConductingEquipment_mRID'], right_on=['name_NODE1', 'name_LINE'],
                            how='left')
    term_results = pd.merge(term_results,
                            output.drop(['p_from_mw', 'q_from_mvar', 'from_bus', 'to_bus', 'name_NODE1'], axis=1),
                            left_on=['name', 'ConductingEquipment_mRID'], right_on=['name', 'name_LINE'], how='left')
    for idx in term_results.index:
        if np.isnan(term_results.at[idx, 'p_to_mw']) == False:
            term_results.at[idx, 'p'] = abs(term_results.at[idx, 'p_to_mw'])
            term_results.at[idx, 'q'] = abs(term_results.at[idx, 'q_to_mvar'])
        if np.isnan(term_results.at[idx, 'p_from_mw']) == False:
            term_results.at[idx, 'p'] = abs(term_results.at[idx, 'p_from_mw'])
            term_results.at[idx, 'q'] = abs(term_results.at[idx, 'q_from_mvar'])
    term_results = term_results.drop(
        ['p_from_mw', 'q_from_mvar', 'name_LINE_x', 'name_NODE1', 'p_to_mw', 'q_to_mvar', 'name_LINE_y'], axis=1)
    return term_results


def result_trafos(trafo, res_trafo, bus, term_results):
    trafo_results = pd.merge(trafo.drop(
        ['std_type', 'sn_mva', 'vn_hv_kv', 'vn_lv_kv', 'vk_percent', 'vkr_percent', 'pfe_kw', 'vector_group',
         'i0_percent', 'shift_degree', 'tap_side', 'tap_neutral', 'tap_min', 'tap_max', 'tap_step_percent',
         'tap_step_degree', 'tap_pos', 'tap_phase_shifter', 'parallel', 'df', 'in_service'], axis=1), res_trafo.drop(
        ['pl_mw', 'ql_mvar', 'i_hv_ka', 'i_lv_ka', 'loading_percent', 'vm_hv_pu', 'va_hv_degree', 'vm_lv_pu',
         'va_lv_degree'], axis=1), left_index=True, right_index=True)
    trafo_results = pd.merge(trafo_results, bus.drop(['vn_kv', 'type', 'zone', 'in_service'], axis=1), left_on="hv_bus",
                             right_index=True)
    trafo_results = pd.merge(trafo_results, bus.drop(['vn_kv', 'type', 'zone', 'in_service'], axis=1), left_on="lv_bus",
                             right_index=True)
    term_results = pd.merge(term_results,
                            trafo_results.drop(['hv_bus', 'lv_bus', 'p_lv_mw', 'q_lv_mvar', 'name'], axis=1),
                            left_on=['name', 'ConductingEquipment_mRID'], right_on=['name_y', 'name_x'], how='left')
    term_results = pd.merge(term_results.drop(['name_x', 'name_y'], axis=1),
                            trafo_results.drop(['hv_bus', 'lv_bus', 'p_hv_mw', 'q_hv_mvar', 'name_y'], axis=1),
                            left_on=['name', 'ConductingEquipment_mRID'], right_on=['name', 'name_x'], how='left')

    for idx in term_results.index:
        # print(output5.at[idx,'p_to_mw'])
        if np.isnan(term_results.at[idx, 'p']) == True:
            if np.isnan(term_results.at[idx, 'p_hv_mw']) == False:
                term_results.at[idx, 'p'] = abs(term_results.at[idx, 'p_hv_mw'])
                term_results.at[idx, 'q'] = abs(term_results.at[idx, 'q_hv_mvar'])
            if np.isnan(term_results.at[idx, 'p_lv_mw']) == False:
                term_results.at[idx, 'p'] = abs(term_results.at[idx, 'p_lv_mw'])
                term_results.at[idx, 'q'] = abs(term_results.at[idx, 'q_lv_mvar'])
    return term_results


def result_loads(load, res_load, term_results):
    load_results = pd.merge(
        load.drop(['p_mw', 'q_mvar', 'const_z_percent', 'const_i_percent', 'sn_mva', 'scaling', 'in_service', 'type'],
                  axis=1), res_load, left_index=True, right_index=True)
    term_results = pd.merge(term_results.drop(['p_hv_mw', 'q_hv_mvar', 'name_x', 'p_lv_mw', 'q_lv_mvar'], axis=1),
                            load_results.drop(['bus'], axis=1), left_on='ConductingEquipment_mRID', right_on='name',
                            how='left')
    for idx in term_results.index:
        if np.isnan(term_results.at[idx, 'p']) == True:
            if np.isnan(term_results.at[idx, 'p_mw']) == False:
                term_results.at[idx, 'p'] = abs(term_results.at[idx, 'p_mw'])
                term_results.at[idx, 'q'] = abs(term_results.at[idx, 'q_mvar'])

        if np.isnan(term_results.at[idx, 'p']) == False:
            term_results.at[idx, 'p'] = int(term_results.at[idx, 'p'] * 1000000)
            term_results.at[idx, 'q'] = int(term_results.at[idx, 'q'] * 1000000)
    return term_results


def geojson_lines(name_bus, b, res_line, line):
    # output = pd.merge(res_voltage.drop(['p_mw','q_mvar'],axis=1), a.drop(['type','zone','in_service'],axis=1),left_index=True, right_index=True)
    output = pd.merge(name_bus, b, left_index=True, right_index=True)
    output = output[output.vm_pu.notnull()]
    # output.drop(['vn_kv','vm_pu','coords']
    output2 = pd.merge(res_line.drop(
        ['pl_mw', 'ql_mvar', 'i_from_ka', 'i_to_ka', 'i_ka', 'vm_from_pu', 'va_from_degree', 'vm_to_pu',
         'va_to_degree'], axis=1), line.drop(
        ['length_km', 'r_ohm_per_km', 'x_ohm_per_km', 'c_nf_per_km', 'g_us_per_km', 'max_i_ka', 'df', 'type',
         'parallel', 'in_service', 'std_type'], axis=1), left_index=True, right_index=True)
    output2 = pd.merge(output2, output.drop(['vm_pu', 'va_degree', 'vn_kv', 'coords'], axis=1), "inner",
                       left_on="from_bus", right_index=True, suffixes=("_LINE", "_NODE1"))
    output2 = pd.merge(output2, output.drop(['vm_pu', 'va_degree', 'vn_kv', 'coords'], axis=1), "inner",
                       left_on="to_bus", right_index=True, suffixes=("_NODE1", "_NODE2"))
    return output2


async def main():
    feeder_mrid = "CPM3B3"
    parser = argparse.ArgumentParser(description="Integration Test of retrieve_network")
    parser.add_argument('server', help='Host and port of grpc server', metavar="host:port", nargs="?",
                        default="ewb.essentialenergy.zepben.com")
    parser.add_argument('--rpc-port', help="The gRPC port for the server", default="9014")
    parser.add_argument('--feeder-mrid', help="The Feeder mRID", default="CPM3B3")
    args = parser.parse_args()
    async with connect_async(host=args.server, rpc_port=args.rpc_port) as channel:
        client = NetworkConsumerClient(channel)
        network = NetworkService()
        result = (await client.get_feeder(network, mrid=args.feeder_mrid)).throw_on_error()
        print(network.get(feeder_mrid))
        p = print_feeder_eq(network)
        bv_11kv = BaseVoltage(mrid='11kv', nominal_voltage=11, name='11kV')
        bv_045kv = BaseVoltage(mrid='0.45kv', nominal_voltage=0.415, name='0.45kV')
        service = NetworkService()
        client = NetworkConsumerClient(channel)
        result = (await client.get_feeder(service, 'CPM3B3')).throw_on_error()
        print(service.get('CPM3B3'))
        net = pp.create_empty_network()
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        print("el resultado es")

        for i in p.index:
            eq = service.get(p.at[i, "name"])
            if p.at[i, "basevol"] == "base-voltage-11000":
                eq.base_voltage = bv_11kv
            if p.at[i, "basevol"] == "base-voltage-415":
                eq.base_voltage = bv_045kv

        term_results = create_bus(service, net)
        term_results.to_csv('Resultss.csv', index=False)
        a = net.bus
        a.to_csv('Bus.csv', index=False)
        b = net.bus_geodata
        create_lines(service, net, a)
        net.line.to_csv('lines.csv', index=False)
        create_switch(service, net, a)
        create_disconnector(service, net, a)
        create_fuse(service, net, a)
        create_recloser(service, net, a)
        create_transformers(service, net, a)
        print(net.trafo)
        Node_head = get_head_feeder(service, a)
        create_generators(service, net, a, Node_head, pd.read_csv('Generator-Input.csv'))
        net.gen.to_csv('Generators.csv', index=False)
        create_load(service, net, a, pd.read_csv('Load-Input.csv'))
        net.load.to_csv('Loads.csv', index=False)
        pp.runpp(net)
        name_bus = name_busbar(net.res_bus, a)
        term_resultsX = result_busbar(name_bus, b, term_results)
        term_resultsX.to_csv('Test.csv', index=True)

        # Begin Test  results geojson lines
        Test = geojson_lines(name_bus, b, net.res_line, net.line)
        Test.to_csv('GeoJSONLINES.csv', index=True)
        # End Test  results geojson lines

        term_resultsX = result_lines(net.res_line, net.line, name_bus, term_resultsX)
        term_resultsX.to_csv('Test2.csv', index=True)
        term_resultsX = result_trafos(net.trafo, net.res_trafo, net.bus, term_resultsX)
        term_resultsX.to_csv('Test3.csv', index=True)

        term_resultsX = result_loads(net.load, net.res_load, term_resultsX)
        term_resultsX.to_csv('Test4.csv', index=False)
        term_resultsX = term_resultsX[np.isnan(term_resultsX['p']) == False]
        geo_results = geopandas.GeoDataFrame(term_resultsX, crs="EPSG:4326",
                                             geometry=geopandas.points_from_xy(term_resultsX.x, term_resultsX.y))
        term_resultsX.to_csv('Test5.csv', index=False)

        # Test GeoJSON

        gdf11 = geopandas.GeoDataFrame(term_resultsX, crs="EPSG:4326")
        gdf11.to_file('Output2.geojson', driver="GeoJSON")

        term_resultsX.insert(1, 'SvVoltage', "")
        term_resultsX.insert(2, 'SvPowerFlow', "")
        for idx in term_resultsX.index:
            term_resultsX.at[idx, 'SvVoltage'] = {"angle": term_resultsX.at[idx, 'va_degree'],
                                                  "v": term_resultsX.at[idx, 'vn_kv'],
                                                  "TopologicalNode_mRID": term_resultsX.at[idx, 'TopologicalNode_mRID']}
            term_resultsX.at[idx, 'SvPowerFlow'] = {"p": term_resultsX.at[idx, 'p'], "q": term_resultsX.at[idx, 'q'],
                                                    "ConductingEquipment_mRID": term_resultsX.at[
                                                        idx, 'ConductingEquipment_mRID']}
        term_resultsX = term_resultsX.drop(
            ['name_x', 'va_degree', 'vn_kv', 'TopologicalNode_mRID', 'ConductingEquipment_mRID', 'p', 'q', 'name_y',
             'p_mw', 'q_mvar', 'x', 'y'], axis=1)
        gdf10 = geopandas.GeoDataFrame(term_resultsX, crs="EPSG:4326")
        gdf10.to_json()
        gdf10.to_file('Output.geojson', driver="GeoJSON")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
