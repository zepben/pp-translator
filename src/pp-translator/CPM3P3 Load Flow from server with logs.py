import argparse
import asyncio
import pandas as pd
import geopandas
import pandapower as pp
import numpy as np
from zepben.evolve import connect_async, NetworkConsumerClient
from zepben.evolve import NetworkService, Feeder, PowerElectronicsConnection, ConnectivityNode, \
    ConductingEquipment, BaseVoltage, AcLineSegment, Junction, EnergyConsumer, \
    Disconnector, Breaker, PowerTransformer, Fuse, Recloser

def conn_to_junction(connectivity_point):
    node = connectivity_point
    for Terminal in connectivity_point._terminals: 
        if type(Terminal._conducting_equipment) == Junction:
            node = Terminal._conducting_equipment
    return node

def tracing(origin, visited): 
    voltage = None
    for segment in origin._terminals: 
        if segment._conducting_equipment.base_voltage is not None: 
            voltage = segment._conducting_equipment.base_voltage.nominal_voltage
            break
        if segment._conducting_equipment not in visited and segment._conducting_equipment.base_voltage is None: 
            visited.append(segment._conducting_equipment)
            for Terminal in segment._conducting_equipment._terminals: 
                if Terminal.connectivity_node != origin:
                    tracing(Terminal.connectivity_node, visited)
    return voltage

def search_voltage(connectivity_point): 
    voltage = None
    for Terminal in connectivity_point._terminals: 
        if voltage is None: 
            if Terminal._conducting_equipment.base_voltage is not None: 
                voltage = Terminal._conducting_equipment.base_voltage.nominal_voltage
            else: 
                voltage = None
    return voltage

def create_bus(service, net): 
    # TODO: Function to obtain BaseVoltage
    data = {'name': [], 'angle': [], 'vn_kv': [], 'TopologicalNode_mRID': [], 'X': [], 'Y': [], 'ConductingEquipment_mRID': []}
    df = pd.DataFrame(data)

    for Term in service.objects(ConnectivityNode): 
        if search_voltage(Term) is not None: 
            for Terminal in Term._terminals: 
                new_row = {'TopologicalNode_mRID': Term.mrid, 'vn_kv':  search_voltage(Term), 'angle': 0, 'name': conn_to_junction(Term).mrid, 'ConductingEquipment_mRID': Terminal._conducting_equipment.mrid}
                df = df.append(new_row, ignore_index=True)

    for equipment in service.objects(ConductingEquipment): 
        if type(equipment) == Fuse or type(equipment) == Breaker or type(equipment) == Disconnector or type(equipment) == Recloser: 
            if len(equipment._terminals) > 2:
                new_row = {'TopologicalNode_mRID': "Term"+equipment.mrid, 'vn_kv':  search_voltage(equipment._terminals[0].connectivity_node), 'angle': 0, 'name': "Term" + equipment.mrid, 'ConductingEquipment_mRID': equipment.mrid}
                df = df.append(new_row, ignore_index=True)

        if type(equipment) == EnergyConsumer and len(equipment._terminals) > 1: 
            new_row = {'TopologicalNode_mRID': "Term"+equipment.mrid, 'vn_kv':  search_voltage(equipment._terminals[0].connectivity_node), 'angle': 0, 'name': "Term" + equipment.mrid, 'ConductingEquipment_mRID': equipment.mrid}
            df = df.append(new_row, ignore_index=True)

    df2 = df.drop(['TopologicalNode_mRID', 'angle', 'ConductingEquipment_mRID'], axis=1)
    df2 = pd.DataFrame.drop_duplicates(df2)
    for idx in df2.index: 
        pp.create_bus(net, name=df2.at[idx, "name"], vn_kv=df2.at[idx, "vn_kv"])
    return df

def print_feeder_eq(service): 
    datavolt = {'name': [], 'type_element': [], 'basevol': []}
    df = pd.DataFrame(datavolt)
    for equip in service.objects(ConductingEquipment): 
        if equip.get_base_voltage() is not None: 
            print(equip.mrid, type(equip).__name__, equip.base_voltage.mrid, equip.base_voltage.nominal_voltage)
            new_row = {'name': equip.mrid, 'type_element': type(equip).__name__, 'basevol': equip.base_voltage.mrid}
            df = df.append(new_row, ignore_index=True)
    return df

def create_switch(service, net, a): 
    for Switch in service.objects(ConductingEquipment): 
        if type(Switch) == Fuse or type(Switch) == Breaker or type(Switch) == Disconnector or type(Switch) == Recloser: 
            if len(Switch._terminals) == 2:
                bus = get_index(conn_to_junction(Switch._terminals[0].connectivity_node).mrid, a)
                element = get_index(conn_to_junction(Switch._terminals[1].connectivity_node).mrid, a)
                pp.create_switch(net, name=Switch.mrid, bus=bus, element=element, et='b', type='CB')
                net.bus_geodata.at[bus, "x"] = Switch.location._position_points[0].x_position
                net.bus_geodata.at[bus, "y"] = Switch.location._position_points[0].y_position
                net.bus_geodata.at[element, "x"] = Switch.location._position_points[0].x_position
                net.bus_geodata.at[element, "y"] = Switch.location._position_points[0].y_position
            elif len(Switch._terminals) > 2: 
                bus = get_index("Term"+Switch.mrid, a)
                for term in Switch._terminals: 
                    element = get_index(conn_to_junction(term.connectivity_node).mrid, a)
                    pp.create_switch(net, name=Switch.mrid + term.mrid, bus=bus, element=element, et='b', type='CB')
                    net.bus_geodata.at[element, "x"] = Switch.location._position_points[0].x_position
                    net.bus_geodata.at[element, "y"] = Switch.location._position_points[0].y_position

def get_index(name_node, nodes):
    dfb = int(nodes[nodes['name'] == name_node].index[0])
    return dfb

def get_base_voltage(service): 
    for Base in service.objects(BaseVoltage): 
        print("The BaseVoltage: " + Base.mrid)

def create_transformers(service, net, a): 
    for Transformer in service.objects(PowerTransformer):
        print("El transssformer es", type(Transformer))
        if (len(Transformer._terminals) ==  2):
                node1 = get_index(conn_to_junction(Transformer._terminals[0].connectivity_node).mrid, a)
                node2 = get_index(conn_to_junction(Transformer._terminals[1].connectivity_node).mrid, a)
                if net.bus.at[node1, "vn_kv"] > net.bus.at[node2, "vn_kv"]:
                    hv_bus, lv_bus = node1, node2
                else: 
                    hv_bus, lv_bus = node2, node1
                sn_mva = Transformer._power_transformer_ends[0].rated_s / 1000000
                vn_hv_kv = 11
                vn_lv_kv = 0.415
                vk_percent = 4
                vkr_percent = 0.4
                pfe_kw = 0.03
                i0_percent = 1
                shift_degree = 0
                pp.create_transformer_from_parameters(net, name=Transformer.mrid, hv_bus=hv_bus, lv_bus=lv_bus, sn_mva=sn_mva, 
                                                      vn_hv_kv=vn_hv_kv, vn_lv_kv=vn_lv_kv, vkr_percent=vkr_percent, 
                                                      vk_percent=vk_percent, pfe_kw=pfe_kw, i0_percent=i0_percent, 
                                                      shift_degree=shift_degree, vector_group="Dyn")
                print("The Power transformers are: " + Transformer.mrid, "|", hv_bus, "|", lv_bus, "|", sn_mva)
                net.bus_geodata.at[hv_bus, "x"] = Transformer.location._position_points[0].x_position
                net.bus_geodata.at[hv_bus, "y"] = Transformer.location._position_points[0].y_position
        else:
                print("Incorrect number terminals")

def create_generators(service, net, a):
    for feederp in service.objects(Feeder):
        head_terminal = get_index(conn_to_junction(feederp.normal_head_terminal.connectivity_node).mrid, a)
        #print("el terminal es"+str(head_terminal)+"/"+feederp.mrid)
        pp.create_ext_grid(net, name="External_Grid", bus=head_terminal, va_degree=0, s_sc_max_mva=1000,
                           s_sc_min_mva=800, rx_max=0.1)
    for eq in service.objects(PowerElectronicsConnection):
        bus = get_index(conn_to_junction(eq._terminals[0].connectivity_node).mrid, a)
        #pp.create_sgen(net, type="PV", name="Gen"+eq.mrid, bus=bus, p_mw=eq.rated_s/1000)
        pp.create_sgen(net, type="PV", name="Gen"+eq.mrid, bus=bus, p_mw=3*eq.p/1000)
        #pp.create_sgen(net, type="PV", name="Gen"+eq.mrid, bus=bus, p_mw=5/1000)
        print(eq.mrid + "/"+str(eq.p)+"/"+str(eq.rated_s))

def create_load(service, net, a, df): 
    # TODO: Validate value loads.
    for Load in service.objects(EnergyConsumer): 
        if len(Load._terminals) == 1:
            bus = get_index(conn_to_junction(Load._terminals[0].connectivity_node).mrid, a)
            for up in list(Load.usage_points): 
                for mt in list(up.end_devices): 
                    for idx in df.index: 
                        if int(df.at[idx, "premnum"]) == int(mt.service_location.name): 
                            p_mw = df.at[idx, "Load"]/1000
                            q_mvar = 0
                            pp.create_load(net, name=Load.mrid, bus=bus, p_mw=p_mw, q_mvar=q_mvar)
                            break
        else: 
            bus = get_index("Term"+Load.mrid, a)
            for up in list(Load.usage_points): 
                for mt in list(up.end_devices): 
                    for idx in df.index: 
                        if int(df.at[idx, "premnum"]) == int(mt.service_location.name): 
                            p_mw = df.at[idx, "Load"]/1000
                            q_mvar = 0
                            pp.create_load(net, name=Load.mrid, bus=bus, p_mw=p_mw, q_mvar=q_mvar)
                            break

            for term in Load._terminals: 
                element = get_index(conn_to_junction(term.connectivity_node).mrid, a)
                pp.create_line_from_parameters(net, name=Load.mrid + "-" + term.mrid, from_bus=element, to_bus=bus, length_km=0.01,
                                           c_nf_per_km=0.001, max_i_ka=0.4, r_ohm_per_km=1,
                                           x_ohm_per_km=1)
                net.bus_geodata.at[element, "x"] = Load.location._position_points[0].x_position
                net.bus_geodata.at[element, "y"] = Load.location._position_points[0].y_position
        net.bus_geodata.at[bus, "x"] = Load.location._position_points[0].x_position
        net.bus_geodata.at[bus, "y"] = Load.location._position_points[0].y_position

def create_lines(service, net, a): 
    lenght_feeder = 0
    for Equip in service.objects(AcLineSegment): 
        if Equip.base_voltage is not None: 
            from_bus = get_index(conn_to_junction(Equip._terminals[0].connectivity_node).mrid, a)
            to_bus = get_index(conn_to_junction(Equip._terminals[1].connectivity_node).mrid, a)
            length_km = Equip.length / 1000
            if length_km < 0.01:
                length_km = 0.011
            lenght_feeder = lenght_feeder + length_km
            r_ohm_per_km = (Equip.per_length_sequence_impedance.r) + 0.1
            x_ohm_per_km = (Equip.per_length_sequence_impedance.x) + 0.1
            c_nf_per_km = (Equip.per_length_sequence_impedance.bch) + 0.1
            if Equip.asset_info.rated_current > 0:
                max_i_ka = Equip.asset_info.rated_current
            elif Equip.base_voltage.nominal_voltage < 1:
                max_i_ka = 0.3
            else: 
                max_i_ka = 0.195
            pp.create_line_from_parameters(net, name=Equip.mrid, from_bus=from_bus, to_bus=to_bus, length_km=length_km, 
                                           c_nf_per_km=c_nf_per_km, max_i_ka=max_i_ka, r_ohm_per_km=r_ohm_per_km, 
                                           x_ohm_per_km=x_ohm_per_km)
            net.bus_geodata.at[from_bus, "x"] = Equip.location._position_points[0].x_position
            net.bus_geodata.at[from_bus, "y"] = Equip.location._position_points[0].y_position
            net.bus_geodata.at[to_bus, "x"] = Equip.location._position_points[1].x_position
            net.bus_geodata.at[to_bus, "y"] = Equip.location._position_points[1].y_position
    print("The Lenght of the feeder is", lenght_feeder)

def name_busbar(res_voltage, a): 
    output = pd.merge(res_voltage.drop(['p_mw', 'q_mvar'], axis=1), a.drop(['type', 'zone', 'in_service'], axis=1), left_index=True, right_index=True)
    return output

def result_busbar(name_bus, b, term_results): 
    #output = pd.merge(res_voltage.drop(['p_mw', 'q_mvar'], axis=1), a.drop(['type', 'zone', 'in_service'], axis=1), left_index=True, right_index=True)
    output = pd.merge(name_bus, b, left_index=True, right_index=True)
    output = output[output.vm_pu.notnull()]
    for idx in output.index: 
        output.at[idx, 'vn_kv'] = int(output.at[idx, 'vn_kv']*output.at[idx, 'vm_pu']*1000)
    output = pd.merge(term_results.drop(['angle', 'vn_kv', 'X', 'Y'], axis=1), output.drop(['vm_pu', 'coords'], axis=1), "inner", left_on="name", right_on="name")
    return output

def result_lines(res_line, line, name_bus, term_results): 
    output = pd.merge(res_line.drop(['loading_percent', 'pl_mw', 'ql_mvar', 'i_from_ka', 'i_to_ka', 'i_ka', 'vm_from_pu', 'va_from_degree', 'vm_to_pu', 'va_to_degree'], axis=1), line.drop(['length_km', 'r_ohm_per_km', 'x_ohm_per_km', 'c_nf_per_km', 'g_us_per_km', 'max_i_ka', 'df', 'type', 'parallel', 'in_service', 'std_type'], axis=1), left_index=True, right_index=True)
    output = pd.merge(output, name_bus.drop(['vm_pu', 'va_degree', 'vn_kv'], axis=1), "inner", left_on="from_bus", right_index=True, suffixes=("_LINE", "_NODE1"))
    output = pd.merge(output, name_bus.drop(['vm_pu', 'va_degree', 'vn_kv'], axis=1), "inner", left_on="to_bus", right_index=True, suffixes=("_NODE1", "_NODE2"))
    term_results = pd.merge(term_results, output.drop(['p_to_mw', 'q_to_mvar', 'from_bus', 'to_bus', 'name'], axis=1), left_on=['name', 'ConductingEquipment_mRID'], right_on=['name_NODE1', 'name_LINE'], how='left')
    term_results = pd.merge(term_results, output.drop(['p_from_mw', 'q_from_mvar', 'from_bus', 'to_bus', 'name_NODE1'], axis=1), left_on=['name', 'ConductingEquipment_mRID'], right_on=['name', 'name_LINE'], how='left')
    for idx in term_results.index: 
        if np.isnan(term_results.at[idx, 'p_to_mw']) == False: 
            term_results.at[idx, 'p'] = abs(term_results.at[idx, 'p_to_mw'])
            term_results.at[idx, 'q'] = abs(term_results.at[idx, 'q_to_mvar'])
        if np.isnan(term_results.at[idx, 'p_from_mw']) == False: 
            term_results.at[idx, 'p'] = abs(term_results.at[idx, 'p_from_mw'])
            term_results.at[idx, 'q'] = abs(term_results.at[idx, 'q_from_mvar'])
    term_results = term_results.drop(['p_from_mw', 'q_from_mvar', 'name_LINE_x', 'name_NODE1', 'p_to_mw', 'q_to_mvar', 'name_LINE_y'], axis=1)
    return term_results

def result_trafos(trafo, res_trafo, bus, term_results): 
    trafo_results = pd.merge(trafo.drop(['std_type', 'sn_mva', 'vn_hv_kv', 'vn_lv_kv', 'vk_percent', 'vkr_percent', 'pfe_kw', 'vector_group', 'i0_percent', 'shift_degree', 'tap_side', 'tap_neutral', 'tap_min', 'tap_max', 'tap_step_percent', 'tap_step_degree', 'tap_pos', 'tap_phase_shifter', 'parallel', 'df', 'in_service'], axis=1), res_trafo.drop(['pl_mw', 'ql_mvar', 'i_hv_ka', 'i_lv_ka', 'loading_percent', 'vm_hv_pu', 'va_hv_degree', 'vm_lv_pu', 'va_lv_degree'], axis=1), left_index=True, right_index=True)
    trafo_results = pd.merge(trafo_results, bus.drop(['vn_kv', 'type', 'zone', 'in_service'], axis=1), left_on="hv_bus", right_index=True)
    trafo_results = pd.merge(trafo_results, bus.drop(['vn_kv', 'type', 'zone', 'in_service'], axis=1), left_on="lv_bus", right_index=True)
    term_results = pd.merge(term_results, trafo_results.drop(['hv_bus', 'lv_bus', 'p_lv_mw', 'q_lv_mvar', 'name'], axis=1), left_on=['name', 'ConductingEquipment_mRID'], right_on=['name_y', 'name_x'], how='left')
    term_results = pd.merge(term_results.drop(['name_x', 'name_y'], axis=1), trafo_results.drop(['hv_bus', 'lv_bus', 'p_hv_mw', 'q_hv_mvar', 'name_y'], axis=1), left_on=['name', 'ConductingEquipment_mRID'], right_on=['name', 'name_x'], how='left')

    for idx in term_results.index: 
        #print(output5.at[idx, 'p_to_mw'])
        if np.isnan(term_results.at[idx, 'p']) == True: 
            if np.isnan(term_results.at[idx, 'p_hv_mw']) == False: 
                term_results.at[idx, 'p'] = abs(term_results.at[idx, 'p_hv_mw'])
                term_results.at[idx, 'q'] = abs(term_results.at[idx, 'q_hv_mvar'])
            if np.isnan(term_results.at[idx, 'p_lv_mw']) == False: 
                term_results.at[idx, 'p'] = abs(term_results.at[idx, 'p_lv_mw'])
                term_results.at[idx, 'q'] = abs(term_results.at[idx, 'q_lv_mvar'])
    return term_results

def result_loads(load, res_load, term_results): 
    load_results = pd.merge(load.drop(['p_mw', 'q_mvar', 'const_z_percent', 'const_i_percent', 'sn_mva', 'scaling', 'in_service', 'type'], axis=1), res_load, left_index=True, right_index=True)
    term_results = pd.merge(term_results.drop(['p_hv_mw', 'q_hv_mvar', 'name_x', 'p_lv_mw', 'q_lv_mvar'], axis=1), load_results.drop(['bus'], axis=1), left_on='ConductingEquipment_mRID', right_on='name', how='left')
    for idx in term_results.index: 
        if np.isnan(term_results.at[idx, 'p']) == True: 
            if np.isnan(term_results.at[idx, 'p_mw']) == False: 
                term_results.at[idx, 'p'] = abs(term_results.at[idx, 'p_mw'])
                term_results.at[idx, 'q'] = abs(term_results.at[idx, 'q_mvar'])
        if np.isnan(term_results.at[idx, 'p']) == False: 
            term_results.at[idx, 'p'] = int(term_results.at[idx, 'p']*1000000)
            term_results.at[idx, 'q'] = int(term_results.at[idx, 'q']*1000000)
    return term_results

def geojson_lines(name_bus, b,  res_line, line): 
    output = pd.merge(name_bus, b, left_index=True, right_index=True)
    output = output[output.vm_pu.notnull()]
    output2 = pd.merge(res_line.drop(['pl_mw', 'ql_mvar', 'i_from_ka', 'i_to_ka', 'i_ka', 'vm_from_pu', 'va_from_degree', 'vm_to_pu', 'va_to_degree'], axis=1), line.drop(['length_km', 'r_ohm_per_km', 'x_ohm_per_km', 'c_nf_per_km', 'g_us_per_km', 'max_i_ka', 'df', 'type', 'parallel', 'in_service', 'std_type'], axis=1), left_index=True, right_index=True)
    output2 = pd.merge(output2, output.drop(['vm_pu', 'va_degree', 'vn_kv', 'coords'], axis=1), "inner", left_on="from_bus", right_index=True, suffixes=("_LINE", "_NODE1"))
    output2 = pd.merge(output2, output.drop(['vm_pu', 'va_degree', 'vn_kv', 'coords'], axis=1), "inner", left_on="to_bus", right_index=True, suffixes=("_NODE1", "_NODE2"))
    for idx in output2.index: 
        output2.at[idx, 'p_from_mw'] = abs(output2.at[idx, 'p_from_mw'])
        output2.at[idx, 'q_from_mvar'] = abs(output2.at[idx, 'q_from_mvar'])
        output2.at[idx, 'p_to_mw'] = abs(output2.at[idx, 'p_to_mw'])
        output2.at[idx, 'q_to_mvar'] = abs(output2.at[idx, 'q_to_mvar'])
    return output2

async def main(): 
    feeder_mrid = "CPM3B3"
    parser = argparse.ArgumentParser(description="Integration Test of retrieve_network")
    parser.add_argument('server', help='Host and port of grpc server', metavar="host: port", nargs="?", 
                        default="ewb.essentialenergy.zepben.com")
    parser.add_argument('--rpc-port', help="The gRPC port for the server", default="9014")
    parser.add_argument('--feeder-mrid', help="The Feeder mRID", default="CPM3B3")
    args = parser.parse_args()
    async with connect_async(host=args.server, rpc_port=args.rpc_port) as channel: 
        client = NetworkConsumerClient(channel)
        network = NetworkService()
        result = (await client.get_feeder(network, mrid=args.feeder_mrid)).throw_on_error()
        print(network.get(feeder_mrid))
        p=print_feeder_eq(network)
        bv_11kv = BaseVoltage(mrid='11kv', nominal_voltage=11, name='11kV')
        bv_045kv = BaseVoltage(mrid='0.45kv', nominal_voltage=0.415, name='0.45kV')
        service = NetworkService()
        client = NetworkConsumerClient(channel)
        result = (await client.get_feeder(service, 'CPM3B3')).throw_on_error()
        #print(service.get('CPM3B3'))
        net = pp.create_empty_network()
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        print("Building Model ...")
        for i in p.index: 
            eq = service.get(p.at[i, "name"])
            if p.at[i, "basevol"] == "base-voltage-11000":
                eq.base_voltage = bv_11kv
            if p.at[i, "basevol"] == "base-voltage-415":
                eq.base_voltage = bv_045kv
        term_results = create_bus(service, net)
        term_results.to_csv('Resultss.csv', index=False)
        a = net.bus
        a.to_csv('Bus.csv', index=True)
        b = net.bus_geodata
        create_lines(service, net, a)
        create_generators(service, net, a)
        create_switch(service, net, a)
        create_transformers(service, net, a)
        net.trafo.to_csv('Trafos.csv', index=True)
        net.switch.to_csv('switch.csv', index=True)
        net.res_sgen.to_csv('Generators.csv', index=True)
        create_load(service, net, a, pd.read_csv('Load-Input.csv'))
        net.load.to_csv('Loads.csv', index=True)
        net.line.to_csv('lines.csv', index=True)
        net.bus_geodata.to_csv('Bus-coord.csv', index=True)
        pp.runpp(net)
        net.res_bus.to_csv('ResultsBus.csv', index=True)
        net.res_line.to_csv("Result_Lines.csv", index=True)
        net.line.to_csv("Result_Lines2.csv", index=True)
        net.res_sgen.to_csv("Results_Generator.csv", index=True)
        net.sgen.to_csv("Results_Generator2.csv", index=True)
        net.res_load.to_csv("Load result.csv", index=True)
        net.res_trafo.to_csv("Results_Transformer.csv", index=True)
        net.res_bus.to_csv("Result_Bus.csv", index=True)

        name_bus = name_busbar(net.res_bus, a)
        term_results_x = result_busbar(name_bus, b, term_results)
        term_results_x.to_csv('Test.csv', index=True)

        #Start Test  results geojson lines
        Test = geojson_lines(name_bus, b, net.res_line, net.line)
        Test.to_csv('GeoJSONLINES.csv', index=True)
        #End Test  results geojson lines

        term_results_x = result_lines(net.res_line, net.line, name_bus, term_results_x)
        term_results_x.to_csv('Test2.csv', index=True)
        term_results_x = result_trafos(net.trafo, net.res_trafo, net.bus, term_results_x)
        term_results_x.to_csv('Test3.csv', index=True)
        term_results_x = result_loads(net.load, net.res_load, term_results_x)
        term_results_x.to_csv('Test4.csv', index=False)
        term_results_x = term_results_x[np.isnan(term_results_x['p']) == False]
        geo_results = geopandas.GeoDataFrame(term_results_x, crs="EPSG: 4326", geometry=geopandas.points_from_xy(term_results_x.x, term_results_x.y))
        term_results_x.to_csv('Test5.csv', index=False)
        #Test GeoJSON
        gdf11 = geopandas.GeoDataFrame(term_results_x, crs="EPSG: 4326")
        gdf11.to_file('Output2.geojson', driver="GeoJSON")
        term_results_x.insert(1, 'SvVoltage', "")
        term_results_x.insert(2, 'SvPowerFlow', "")
        for idx in term_results_x.index: 
            term_results_x.at[idx, 'SvVoltage'] = {"angle": term_results_x.at[idx, 'va_degree'], "v": term_results_x.at[idx, 'vn_kv'], "TopologicalNode_mRID": term_results_x.at[idx, 'TopologicalNode_mRID']}
            term_results_x.at[idx, 'SvPowerFlow'] = {"p": term_results_x.at[idx, 'p'], "q": term_results_x.at[idx, 'q'], "ConductingEquipment_mRID": term_results_x.at[idx, 'ConductingEquipment_mRID']}
        term_results_x = term_results_x.drop(['name_x', 'va_degree', 'vn_kv', 'TopologicalNode_mRID', 'ConductingEquipment_mRID', 'p', 'q', 'name_y', 'p_mw', 'q_mvar', 'x', 'y'], axis=1)
        gdf10 = geopandas.GeoDataFrame(term_results_x, crs="EPSG: 4326")
        gdf10.to_json()
        gdf10.to_file('Output.geojson', driver="GeoJSON")
        pp.diagnostic(net)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
