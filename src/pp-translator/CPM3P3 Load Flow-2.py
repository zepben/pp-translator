import asyncio

from geopandas import GeoDataFrame
from zepben.evolve import connect_async, NetworkConsumerClient
from zepben.evolve import NetworkService, Feeder, EnergySource, EnergySourcePhase, ConnectivityNode, \
    ConductingEquipment, BaseVoltage, Terminal, EquipmentContainer, AcLineSegment, Junction, EnergyConsumer, \
    Disconnector, Breaker, PowerTransformer, Location,Fuse, Recloser, DiagramObjectPoint,PositionPoint, basevoltage_to_cim, EnergyConsumerPhase
import zepben.evolve
import pandas as pd
import pandapower as pp
import pandapower.plotting as plot
import seaborn
from pandapower.plotting import simple_plot, simple_plotly, pf_res_plotly
import geopandas
import contextily as cx
import pandapower.networks as nw
import matplotlib.pyplot as plt
import geoplot as gplt
import mapclassify
from shapely.geometry import LineString, Point,MultiLineString
colors=seaborn.color_palette()

'''Coord_df = {'ID_BUS': [], 'X': [], 'Y': []}
df = pd.DataFrame(Coord_df,index,index="ID_BUS")


for Term in service.objects(ConnectivityNode):
    vn_kv = LV
    if Term.mrid == "SourceNode":
        vn_kv = MV
        print("-", Term.mrid, "-", Term._terminals[0].phases.name)
    new_row = {'name': conn_to_junction(Term).mrid, 'vn_kv': MV}
    df = df.append(new_row, ignore_index=True)
df = pd.DataFrame.drop_duplicates(df)

Coord_datafame'''
MV = 11000 / 1000
LV = 11000 / 1000

'''def locations(service):
    c=0
    for geo in service.objects(ConductingEquipment):
        print(geo.mrid,"|",geo.Location)
        #print("comienza")
        #c=c+1
        #for attr in dir(geo):
            #print("geo.%s = %r" % (attr, getattr(geo, attr)))
    print("es",c)'''
def conn_to_junction(Connectivity_Point):
    # TODO: Solve problem of connectivity when there are fuses with more than 2 terminals
    Node = Connectivity_Point
    for Terminal in Connectivity_Point._terminals:
        if (type(Terminal._conducting_equipment) == zepben.evolve.model.cim.iec61970.base.wires.connectors.Junction):
            Node = Terminal._conducting_equipment
    return Node


def create_disconnector(service, net, a):
    for Switch in service.objects(Disconnector):
        if len(Switch._terminals) == 2:
            bus = get_index(conn_to_junction(Switch._terminals[0].connectivity_node).mrid, a)
            element = get_index(conn_to_junction(Switch._terminals[1].connectivity_node).mrid, a)
            et = 'b'
            type = "CB"
            pp.create_switch(net, name=Switch.mrid, bus=bus, element=element, et=et, type=type)
            # print("The Switch:"+ Switch.mrid,"|",Switch.in_service)
            net.bus_geodata.at[bus,"x"]=(Switch.location._position_points[0].x_position)
            net.bus_geodata.at[bus,"y"]=(Switch.location._position_points[0].y_position)
            net.bus_geodata.at[element,"x"]=(Switch.location._position_points[0].x_position)
            net.bus_geodata.at[element,"y"]=(Switch.location._position_points[0].y_position)
def create_fuse(service, net, a):
    for Switch in service.objects(Fuse):
        bus = get_index(conn_to_junction(Switch._terminals[0].connectivity_node).mrid, a)
        element = get_index(conn_to_junction(Switch._terminals[1].connectivity_node).mrid, a)
        et = 'b'
        type = "CB"
        pp.create_switch(net, name=Switch.mrid, bus=bus, element=element, et=et, type=type)
        # print("The fuses:"+ Switch.mrid,"|",Switch.in_service)
        net.bus_geodata.at[bus,"x"]=(Switch.location._position_points[0].x_position)
        net.bus_geodata.at[bus,"y"]=(Switch.location._position_points[0].y_position)
        net.bus_geodata.at[element,"x"]=(Switch.location._position_points[0].x_position)
        net.bus_geodata.at[element,"y"]=(Switch.location._position_points[0].y_position)
def create_recloser(service, net, a):
    for Switch in service.objects(Recloser):
        if len(Switch._terminals) == 2:
            bus = get_index(conn_to_junction(Switch._terminals[0].connectivity_node).mrid, a)
            element = get_index(conn_to_junction(Switch._terminals[1].connectivity_node).mrid, a)
            et = 'b'
            type = "CB"
            pp.create_switch(net, name=Switch.mrid, bus=bus, element=element, et=et, type=type)
            # print("The reclosers:"+ Switch.mrid,"|",Switch.in_service)
            net.bus_geodata.at[bus,"x"]=(Switch.location._position_points[0].x_position)
            net.bus_geodata.at[bus,"y"]=(Switch.location._position_points[0].y_position)
            net.bus_geodata.at[element,"y"]=(Switch.location._position_points[0].y_position)
            net.bus_geodata.at[element,"y"]=(Switch.location._position_points[0].y_position)


def get_index(Nombre_Nodo, Nodos):
    dfb = int(Nodos[Nodos['name'] == Nombre_Nodo].index[0])
    return dfb


def get_head_feeder(service, a):
    for Feederp in service.objects(Feeder):
        Nodo_Cabecera = get_index(conn_to_junction(Feederp.normal_head_terminal.connectivity_node).mrid, a)
        return (Nodo_Cabecera)


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
        net.bus_geodata.at[bus,"x"]=(Switch.location._position_points[0].x_position)
        net.bus_geodata.at[bus,"y"]=(Switch.location._position_points[0].y_position)
        net.bus_geodata.at[element,"x"]=(Switch.location._position_points[0].x_position)
        net.bus_geodata.at[element,"y"]=(Switch.location._position_points[0].y_position)



def create_transformers(service, net, a):
    # TODO:Validate power transformers parameters required to load flow
    for Transformer in service.objects(PowerTransformer):
        if (len(Transformer._terminals) == 2):
            hv_bus = get_index(conn_to_junction(Transformer._terminals[0].connectivity_node).mrid, a)
            lv_bus = get_index(conn_to_junction(Transformer._terminals[1].connectivity_node).mrid, a)
            sn_mva = Transformer._power_transformer_ends[0].rated_s / 1000000
            vn_hv_kv = MV
            vn_lv_kv = LV
            vk_percent = 1
            vkr_percent = 0.2
            pfe_kw = 0.03
            i0_percent = 1
            shift_degree = 0
            pp.create_transformer_from_parameters(net, name=Transformer.mrid, hv_bus=hv_bus, lv_bus=lv_bus, sn_mva=0.01,
                                                  vn_hv_kv=vn_hv_kv, vn_lv_kv=vn_lv_kv, vkr_percent=vkr_percent,
                                                  vk_percent=vk_percent, pfe_kw=pfe_kw, i0_percent=i0_percent,
                                                  shift_degree=shift_degree, vector_group="Dyn")



            print("The Power transformers are:" + Transformer.mrid, "|", hv_bus, "|", lv_bus, "|", sn_mva)
            net.bus_geodata.at[hv_bus,"x"]=(Transformer.location._position_points[0].x_position)
            net.bus_geodata.at[hv_bus,"y"]=(Transformer.location._position_points[0].y_position)
            #net.bus_geodata.at[lv_bus,"x"]=(Transformer.location._position_points[1].x_position)
            #net.bus_geodata.at[lv_bus,"y"]=(Transformer.location._position_points[1].y_position)

def create_bus(service, net):
    # TODO:Function to obtain BaseVoltage
    MV = 11000 / 1000
    LV = 11000 / 1000
    data = {'name': [], 'vn_kv': [],'X':[],'Y':[]}
    df = pd.DataFrame(data)

    for Term in service.objects(ConnectivityNode):
        vn_kv = LV
        if Term.mrid == "SourceNode":
            vn_kv = MV
            print("-", Term.mrid, "-", Term._terminals[0].phases.name)
        new_row = {'name': conn_to_junction(Term).mrid, 'vn_kv': MV}
        df = df.append(new_row, ignore_index=True)
    df = pd.DataFrame.drop_duplicates(df)
    for idx in df.index:
        pp.create_bus(net, name=df.at[idx, "name"], vn_kv=df.at[idx, "vn_kv"])
    print(len(df))
    return df



def create_generators(service, net, a, Nodo_Cabecera):
    # TODO:Validate generators parameters required to load flow (Actually these values are constants)
    for Ext in service.objects(EnergySource):
        bus = get_index(conn_to_junction(Ext._terminals[0].connectivity_node).mrid, a)
        if bus == Nodo_Cabecera:
            p_mw = Ext.p_max + 1
            pp.create_ext_grid(net, bus=bus, name=Ext.mrid, va_degree=0, s_sc_max_mva=1000, s_sc_min_mva=800,
                               rx_max=0.1, r0x0_max=1, x0x_max=1, r0x0_min=0.1, x0x_min=1)
            print("Las redes externas son:" + Ext.mrid, "|", bus, Ext.voltage_magnitude, p_mw)
            #net.bus_geodata.at[Nodo_Cabecera,"x"]=152.897304
            #net.bus_geodata.at[Nodo_Cabecera,"y"]=-31.4609845


    if net.ext_grid.empty == True:
        # pp.create_ext_grid(net,name="External_Grid",bus=Nodo_Cabecera,p_mw=10,va_degree=0,s_sc_max_mva=1000,s_sc_min_mva=800,rx_max=0.1)
        pp.create_ext_grid(net, name="External_Grid", bus=Nodo_Cabecera, va_degree=0, s_sc_max_mva=1000,
                           s_sc_min_mva=800, rx_max=0.1)
        #net.bus_geodata.at[Nodo_Cabecera,"x"]=152.897304
        #net.bus_geodata.at[Nodo_Cabecera,"y"]=-31.4609845
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

        if bus != Nodo_Cabecera:
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


def create_load(service, net, a):
    # TODO:Validate value loads.
    # This case is when the phases are defined.
    for Load in service.objects(EnergyConsumer):
        Fases = Load._terminals[0].phases.name
        bus = get_index(conn_to_junction(Load._terminals[0].connectivity_node).mrid, a)
        p_mw = (Load.p / 1000000) + 0.001
        q_mvar = (Load.q / 1000000) + 0.0003
        if Fases == "A":
            pp.create_asymmetric_load(net, name=Load.mrid, bus=bus, p_a_mw=p_mw, q_a_mvar=q_mvar, type='wye')
        elif Fases == "B":
            pp.create_asymmetric_load(net, name=Load.mrid, bus=bus, p_b_mw=p_mw, q_b_mvar=q_mvar, type='wye')
        elif Fases == "C":
            pp.create_asymmetric_load(net, name=Load.mrid, bus=bus, p_c_mw=p_mw, q_c_mvar=q_mvar, type='wye')
        elif Fases == "ABC" or Fases == "ABCN":
            pp.create_load(net, name=Load.mrid, bus=bus, p_mw=p_mw, q_mvar=q_mvar)


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
    longitud=0
    for Equip in service.objects(AcLineSegment):
        from_bus = get_index(conn_to_junction(Equip._terminals[0].connectivity_node).mrid, a)
        to_bus = get_index(conn_to_junction(Equip._terminals[1].connectivity_node).mrid, a)
        length_km = Equip.length / 1000
        if length_km<0.01:
            length_km=0.01
        longitud=longitud+length_km
        # Se debe borrar el *1.000 están mal los valores de r y x
        r_ohm_per_km = (Equip.per_length_sequence_impedance.r) + 0.001
        x_ohm_per_km = (Equip.per_length_sequence_impedance.x) + 0.001
        c_nf_per_km = (Equip.per_length_sequence_impedance.bch) + 0.001
        # print(Equip.per_length_sequence_impedance.r,"|", length_km,"|",Equip.per_length_sequence_impedance.r*length_km,"|",r_ohm_per_km)
        r0_ohm_per_km = (Equip.per_length_sequence_impedance.r0) + 0.001
        x0_ohm_per_km = (Equip.per_length_sequence_impedance.x0) + 0.001
        c0_nf_per_km = (Equip.per_length_sequence_impedance.x0) + 0.001
        max_i_ka = 1000
        pp.create_line_from_parameters(net, name=Equip.mrid, from_bus=from_bus, to_bus=to_bus, length_km=length_km,
                                       c_nf_per_km=c_nf_per_km, max_i_ka=max_i_ka, r_ohm_per_km=r_ohm_per_km,
                                       x_ohm_per_km=x_ohm_per_km)
        #print(Equip.location._position_points[0].x_position)
        net.bus_geodata.at[from_bus,"x"]=(Equip.location._position_points[0].x_position)
        net.bus_geodata.at[from_bus,"y"]=(Equip.location._position_points[0].y_position)
        net.bus_geodata.at[to_bus,"x"]=(Equip.location._position_points[1].x_position)
        net.bus_geodata.at[to_bus,"y"]=(Equip.location._position_points[1].y_position)
    print("la longitud es",longitud)
async def main():
    async with connect_async(host="localhost", rpc_port=50052) as channel:
        service = NetworkService()
        client = NetworkConsumerClient(channel)
        result = (await client.get_feeder(service, 'CPM3B3')).throw_on_error()
        print(service.get('CPM3B3'))
        net = pp.create_empty_network()
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        #locations(service)
        create_bus(service, net)
        a = net.bus
        b=net.bus_geodata
        print("The busbar:")
        print(a)
        #for idx in a.index:
        #    a.at[idx,"vn_kv"]=12
        #print(a)
        create_lines(service, net, a)
        print("The lines:")
        print(net.line)
        net.line.to_csv('lines.csv', index=False)
        print("The switch:")
        create_switch(service, net, a)
        print("The disconnector:")
        create_disconnector(service, net, a)
        print("The fuses:")
        create_fuse(service, net, a)
        print("The Recloser:")
        create_recloser(service, net, a)
        print("The disconnector+fuse+recloser+switch:")
        print(net.switch)
        create_transformers(service, net, a)
        print("The transformers:")
        print(net.trafo)
        net.trafo.to_csv('trafos.csv', index=False)
        Nodo_Cabecera = get_head_feeder(service, a)
        print("The head terminal:", Nodo_Cabecera)
        create_generators(service, net, a, Nodo_Cabecera)
        print("The External Grid:")
        print(net.ext_grid)
        print("The Generators:")
        print(net.gen)
        create_load(service, net, a)
        print("The loads:")
        print(net.load)
        net.load.to_csv('loads.csv', index=False)
        #print(b)
        pp.runpp(net)
        print("The voltage results are:")
        res_voltage = net.res_bus.vm_pu
        print(res_voltage)
        print("The loading results are:")
        print(net.res_line.loading_percent)
        #Drop delete columns
        a=a.drop(['zone', 'in_service'], axis=1)
        #del a['zone']
        output = pd.merge(res_voltage, a,left_index=True, right_index=True)
        output.to_csv('fileLocation.csv', index=True)
        output2=pd.merge(a, b,left_index=True, right_index=True)
        output2.to_csv('fileLocation2.csv', index=True)
        #output2=output2.drop(['name','vn_kv','type'],axis=1)
        output3=pd.merge(output,output2.drop(['name','vn_kv','type'],axis=1),"left",left_index=True,right_index=True)
        #Filter NaN Voltage
        output3=output3[output3.vm_pu.notnull()]
        output3.to_csv('Results_Bus.csv', index=True)
        gdf = geopandas.GeoDataFrame(output3,crs="EPSG:4326",geometry=geopandas.points_from_xy(output3.x, output3.y))
        #gdf.style.background_gradient(cmap='Blues')
        scheme = mapclassify.Quantiles(output3['vm_pu'], k=5)
        gplt.pointplot(gdf,hue='vm_pu',scheme=scheme,legend=True,legend_var='hue',cmap='Blues')


        #gdf.plot()
        #gdf = gdf.set_cr
        #gdf = gdf.set_crs(epsg=4326)
        #gdf.set_crs(epsg=4326, inplace=True)
        #gdf= gdf.to_crs(epsg=3395)
        print("geoJSones")
        print(gdf.head)
        #ax = gdf.to_crs('EPSG:3857').plot(color="red", edgecolor="black",markersize=50,figsize=(9, 9))

        #ax.set_title('Bay Area Bart Stations')

        #cx.add_basemap(ax)
        #world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        #output3 = output3.to_crs(epsg=4326)
        #ax = gdf_new.plot(color="red", figsize=(9, 9))
        #ax = gdf.to_crs('EPSG:3857').plot(figsize=(9, 9))
        #print(gdf.crs)
        #cx.add_basemap(ax)

        #Extraer cargabilidad líneas
        s=pd.merge(net.res_line.loading_percent, net.line,left_index=True, right_index=True)
        s=s.drop(['parallel','in_service'],axis=1)
        s.to_csv('Results_Lines.csv', index=False)
        p=pd.merge(s, output3.drop(['type','coords','geometry'],axis=1)   ,"inner", left_on="from_bus", right_index=True,suffixes=("_LINE","_NODE1"))
        p.to_csv('Results_Lines2.csv', index=False)
        q=pd.merge(p,output3.drop(['type','coords','geometry'],axis=1),"inner", left_on="to_bus", right_index=True,suffixes=("_NODE1","_NODE2"))
        q.to_csv('Results_Lines3.csv', index=False)
        gdf.to_json()
        gdf.to_file('dataframe.geojson', driver="GeoJSON")
        #plt.show()
        #output3.insert(loc=len(output3.columns),column='geometry2',value='')
        #for idx in output3.index:
            #output3.at[idx,"geometry2"]=LineString([(output3.at[idx,"x"],output3.at[idx,"y"]), (output3.at[idx,"x"],output3.at[idx,"y"])])
        q.insert(loc=len(q.columns),column='geometry',value='')
        for idx in q.index:
            q.at[idx,"geometry"]=LineString([(q.at[idx,"x_NODE1"],q.at[idx,"y_NODE1"]), (q.at[idx,"x_NODE2"],q.at[idx,"y_NODE2"])])
        print(q)
        gdf2 = geopandas.GeoDataFrame(q, crs="EPSG:4326")
        gdf2.to_json()
        gdf2.to_file('dataframe-line.geojson', driver="GeoJSON")
        #M=MultiLineString
        #for idx in output3.index:
            #line=LineString([(output3.at[idx,"x"],output3.at[idx,"y"]), (output3.at[idx,"x"],output3.at[idx,"y"])])
        #line=LineString([(q.x_x,q.y_x,0), (q.x_y,q.y_y,0)])

        #gdf2 = geopandas.GeoDataFrame(q,crs="EPSG:4326",geometry=LineString ([(q.x_x,q.y_x), (q.x_y,q.y_y)]))
        #print(line)
        #print()
        #simple_plotly(net,on_map=True,projection='epsg:4326')
        #crs={'init':'espg:4326'}
        #geometry=[Point(xy)for xy in  zip (output["x"],output["y"])]
        #geometry[:3]

        #df_geo=gpd.GeoDataFrame(output2,geometry=gpd.points_from_xy(output2.x,output2.y))
        #print(df_geo)
        #df_geo.plot(ax=axis,color='black')

        #net=nw.mv_oberrhein()
        #bc=plot.create_bus_collection(net,buses=net.bus.index,color=colors[0],size=80,zorder=10)
        #lc=plot.create_line_collection(net, lines=net.line.index,color='grey',zorder=2)
        #plot.draw_collections([bc])
        #plt.show()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())










