import asyncio
from zepben.evolve import connect_async, NetworkConsumerClient
from zepben.evolve import NetworkService, Feeder,EnergySource,EnergySourcePhase,ConnectivityNode, ConductingEquipment, BaseVoltage, Terminal, EquipmentContainer,AcLineSegment,Junction,EnergyConsumer,Disconnector,Breaker,PowerTransformer,Fuse,Recloser,basevoltage_to_cim,EnergyConsumerPhase
import zepben.evolve
import pandas as pd
import pandapower as pp
MV=11000/1000
LV=11000/1000

def conn_to_junction(Connectivity_Point):
    #TODO: Solve problem of connectivity when there are fuses with more than 2 terminals
    Node=Connectivity_Point
    for Terminal in Connectivity_Point._terminals:
        if(type(Terminal._conducting_equipment)==zepben.evolve.model.cim.iec61970.base.wires.connectors.Junction):
            Node=Terminal._conducting_equipment
    return Node

def create_disconnector(service,net,a):
    for Switch in service.objects(Disconnector):
        if len(Switch._terminals)==2:
            bus=get_index(conn_to_junction(Switch._terminals[0].connectivity_node).mrid,a)
            element=get_index(conn_to_junction(Switch._terminals[1].connectivity_node).mrid,a)
            et='b'
            type="CB"
            pp.create_switch(net,name=Switch.mrid,bus=bus,element=element,et=et,type=type)
            #print("The Switch:"+ Switch.mrid,"|",Switch.in_service)

def create_fuse(service,net,a):
    for Switch in service.objects(Fuse):
        bus=get_index(conn_to_junction(Switch._terminals[0].connectivity_node).mrid,a)
        element=get_index(conn_to_junction(Switch._terminals[1].connectivity_node).mrid,a)
        et='b'
        type="CB"
        pp.create_switch(net,name=Switch.mrid,bus=bus,element=element,et=et,type=type)
        #print("The fuses:"+ Switch.mrid,"|",Switch.in_service)

def create_recloser(service,net,a):
    for Switch in service.objects(Recloser):
        if len(Switch._terminals)==2:
            bus=get_index(conn_to_junction(Switch._terminals[0].connectivity_node).mrid,a)
            element=get_index(conn_to_junction(Switch._terminals[1].connectivity_node).mrid,a)
            et='b'
            type="CB"
            pp.create_switch(net,name=Switch.mrid,bus=bus,element=element,et=et,type=type)
            #print("The reclosers:"+ Switch.mrid,"|",Switch.in_service)

def get_index(Nombre_Nodo,Nodos):
    dfb = int(Nodos[Nodos['name']==Nombre_Nodo].index[0])
    return dfb

def get_head_feeder(service,a):
    for Feederp in service.objects(Feeder):
        Nodo_Cabecera=get_index(conn_to_junction(Feederp.normal_head_terminal.connectivity_node).mrid,a)
        return(Nodo_Cabecera)

def get_base_voltage(service):
    for Base in service.objects(BaseVoltage):
        print("The BaseVoltage:" +Base.mrid)

def get_energy_source_phases(service):
    for PhaseGen in service.objects(EnergySourcePhase):
        print("The EnergySourcePhases",PhaseGen.mrid,PhaseGen.name)

def get_energy_consumer_phases(service):
    for PhaseLoads in service.objects(EnergyConsumerPhase):
        print("The EnergyConsumerPhases",PhaseLoads.mrid,PhaseLoads.name)

def create_switch(service,net,a):
    for Switch in service.objects(Breaker):
        bus=get_index(conn_to_junction(Switch._terminals[0].connectivity_node).mrid,a)
        element=get_index(conn_to_junction(Switch._terminals[1].connectivity_node).mrid,a)
        et='b'
        type="CB"
        pp.create_switch(net,name=Switch.mrid,bus=bus,element=element,et=et,type=type)
        #print("The Switch in Pandapower are:"+ Switch.mrid,"|",Switch.in_service)

def create_transformers(service,net,a):
    #TODO:Validate power transformers parameters required to load flow
    for Transformer in service.objects(PowerTransformer):
        if(len(Transformer._terminals)==2):
            hv_bus=get_index(conn_to_junction(Transformer._terminals[0].connectivity_node).mrid,a)
            lv_bus=get_index(conn_to_junction(Transformer._terminals[1].connectivity_node).mrid,a)
            sn_mva=Transformer._power_transformer_ends[0].rated_s/1000000
            vn_hv_kv=MV
            vn_lv_kv=LV
            vk_percent=3
            vkr_percent=0.3
            pfe_kw=0.03
            i0_percent=1
            shift_degree=330
            pp.create_transformer_from_parameters(net,name=Transformer.mrid,hv_bus=hv_bus,lv_bus=lv_bus,sn_mva=0.01,vn_hv_kv=vn_hv_kv,vn_lv_kv=vn_lv_kv,vkr_percent=vkr_percent,vk_percent=vk_percent,pfe_kw=pfe_kw,i0_percent=i0_percent,shift_degree=shift_degree,vector_group="Dyn",vk0_percent=4.01995, vkr0_percent=0.4,mag0_percent=100,mag0_rx=0,si0_hv_partial=0.9)
            print("The Power transformers are:"+Transformer.mrid,"|",hv_bus,"|",lv_bus,"|",sn_mva)

def create_bus(service,net):
    #TODO:Function to obtain BaseVoltage
    MV=11000/1000
    LV=11000/1000
    data= {'name':[],'Voltage':[]}
    df=pd.DataFrame(data)

    for Term in service.objects(ConnectivityNode):
        vn_kv=LV
        if Term.mrid=="SourceNode":
            vn_kv=MV
            print("-",Term.mrid,"-",Term._terminals[0].phases.name)
        new_row={'name':conn_to_junction(Term).mrid, 'vn_kv':MV}
        df = df.append(new_row, ignore_index=True)
    df=pd.DataFrame.drop_duplicates(df)

    for idx in df.index:
        pp.create_bus(net,name=df.at[idx,"name"],vn_kv=df.at[idx,"vn_kv"])
    print(len(df))

def create_generators(service,net,a,Nodo_Cabecera):
    #TODO:Validate generators parameters required to load flow (Actually these values are constants)
    for Ext in service.objects(EnergySource):
        bus=get_index(conn_to_junction(Ext._terminals[0].connectivity_node).mrid,a)
        if bus==Nodo_Cabecera:
            p_mw=Ext.p_max+1
            pp.create_ext_grid(net,bus=bus,name=Ext.mrid,p_mw=p_mw,va_degree=0,s_sc_max_mva=1000,s_sc_min_mva=800,rx_max=0.1,r0x0_max=1, x0x_max=1, r0x0_min=0.1, x0x_min=1)
            print("Las redes externas son:" + Ext.mrid,"|",bus,Ext.voltage_magnitude,p_mw)
    if net.ext_grid.empty==True:
        #pp.create_ext_grid(net,name="External_Grid",bus=Nodo_Cabecera,p_mw=10,va_degree=0,s_sc_max_mva=1000,s_sc_min_mva=800,rx_max=0.1)
        pp.create_ext_grid(net,name="External_Grid",bus=Nodo_Cabecera,va_degree=0,s_sc_max_mva=1000,s_sc_min_mva=800,rx_max=0.1)

    for Generators in service.objects(EnergySource):
        print("El generador es"+Generators.mrid)
        Fases=Generators._terminals[0].phases.name
        bus=get_index(conn_to_junction(Generators._terminals[0].connectivity_node).mrid,a)
        try:
            p_mw=Generators.active_power+0.002
        except:
            p_mw=0.002
        try:
            q_mvar=Generators.reactive_power+0.001
        except:
            q_mvar=0.0001
        sn_mva=Generators.p_max
        angle=Generators.voltage_angle
        slack=False
        type="PV"

        if bus!=Nodo_Cabecera:
            if Fases=="A":
                pp.create_asymmetric_sgen(net,name=Generators.mrid,bus=bus,p_a_mw=p_mw,q_a_mvar=q_mvar, sn_mva=sn_mva,type=type)
            elif Fases=="B":
                pp.create_asymmetric_sgen(net,name=Generators.mrid,bus=bus,p_b_mw=p_mw,q_b_mvar=q_mvar, sn_mva=sn_mva,type=type)
            elif Fases=="C":
                pp.create_asymmetric_sgen(net,name=Generators.mrid,bus=bus,p_c_mw=p_mw,q_c_mvar=q_mvar, sn_mva=sn_mva,type=type)
            elif Fases=="ABC" or Fases=="ABCN":
                pp.create_gen(net,name=Generators.mrid,bus=bus,p_mw=p_mw)
            #print("2")
            print("Los generadores son:" + Generators.mrid,"|",bus,Generators.voltage_magnitude,sn_mva,angle)
    #print(net.gen)


def create_load(service,net,a):
    #TODO:Validate value loads.
    #This case is when the phases are defined.
    for Load in service.objects(EnergyConsumer):
        Fases=Load._terminals[0].phases.name
        bus= get_index(conn_to_junction(Load._terminals[0].connectivity_node).mrid,a)
        p_mw=(Load.p/1000000)+0.001
        q_mvar=(Load.q/1000000)+0.0003
        if Fases=="A":
            pp.create_asymmetric_load(net,name=Load.mrid,bus=bus,p_a_mw=p_mw,q_a_mvar=q_mvar,type='wye')
        elif Fases=="B":
            pp.create_asymmetric_load(net,name=Load.mrid,bus=bus,p_b_mw=p_mw,q_b_mvar=q_mvar,type='wye')
        elif Fases=="C":
            pp.create_asymmetric_load(net,name=Load.mrid,bus=bus,p_c_mw=p_mw,q_c_mvar=q_mvar,type='wye')
        elif Fases=="ABC" or Fases=="ABCN":
            pp.create_load(net,name=Load.mrid,bus=bus,p_mw=p_mw,q_mvar=q_mvar)

def create_load_with_phases(service,net,a):
    #This function create randomly the phases when these are not defined
    c=0
    for Load in service.objects(EnergyConsumer):
        Fases=Load._terminals[0].phases.name
        bus= get_index(conn_to_junction(Load._terminals[0].connectivity_node).mrid,a)
        p_mw=(Load.p/1000000)+0.001
        q_mvar=(Load.q/1000000)+0.0003
        module=c % 3
        if module==0:
            pp.create_asymmetric_load(net,name=Load.mrid,bus=bus,p_a_mw=p_mw,q_a_mvar=q_mvar,type='wye')
        elif module==1:
            pp.create_asymmetric_load(net,name=Load.mrid,bus=bus,p_b_mw=p_mw,q_b_mvar=q_mvar,type='wye')
        elif module==2:
            pp.create_asymmetric_load(net,name=Load.mrid,bus=bus,p_c_mw=p_mw,q_c_mvar=q_mvar,type='wye')
        c+=1

def create_lines(service,net,a):
    #TODO:Validate parameters of the lines.
    for Equip in service.objects(AcLineSegment):
        from_bus=get_index(conn_to_junction(Equip._terminals[0].connectivity_node).mrid,a)
        to_bus=get_index(conn_to_junction(Equip._terminals[1].connectivity_node).mrid,a)
        length_km=Equip.length/1000
        #Se debe borrar el *1.000 están mal los valores de r y x
        r_ohm_per_km=(Equip.per_length_sequence_impedance.r)+0.001
        x_ohm_per_km=(Equip.per_length_sequence_impedance.x)+0.001
        c_nf_per_km=(Equip.per_length_sequence_impedance.bch)+0.001
        #print(Equip.per_length_sequence_impedance.r,"|", length_km,"|",Equip.per_length_sequence_impedance.r*length_km,"|",r_ohm_per_km)
        r0_ohm_per_km=(Equip.per_length_sequence_impedance.r0)+0.001
        x0_ohm_per_km=(Equip.per_length_sequence_impedance.x0)+0.001
        c0_nf_per_km=(Equip.per_length_sequence_impedance.x0)+0.001
        max_i_ka=1000
        pp.create_line_from_parameters(net,name=Equip.mrid,from_bus=from_bus,to_bus=to_bus,length_km=0.01, c_nf_per_km=c_nf_per_km,max_i_ka=max_i_ka,r_ohm_per_km=0.001,x_ohm_per_km=0.001)

async def main():
    async with connect_async(host="localhost", rpc_port=50052) as channel:
        service = NetworkService()
        client = NetworkConsumerClient(channel)
        result = (await client.get_feeder(service, 'CPM3B3')).throw_on_error()
        print(service.get('CPM3B3'))
        net=pp.create_empty_network()
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows',None)
        create_bus(service,net)
        a=net.bus
        print("The busbar:")
        print(a)
        create_lines(service,net,a)
        print("The lines:")
        print(net.line)
        print("The switch:")
        create_switch(service,net,a)
        print("The disconnector:")
        create_disconnector(service,net,a)
        print("The fuses:")
        create_fuse(service,net,a)
        print("The Recloser:")
        create_recloser(service,net,a)
        print("The disconnector+fuse+recloser+switch:")
        print(net.switch)
        create_transformers(service,net,a)
        print("The transformers:")
        print(net.trafo)
        Nodo_Cabecera= get_head_feeder(service,a)
        print("The head terminal:",Nodo_Cabecera)
        create_generators(service,net,a,Nodo_Cabecera)
        print("The External Grid:")
        print(net.ext_grid)
        print("The Generators:")
        print(net.gen)
        create_load(service,net,a)
        print("The loads:")
        print(net.load)
        pp.runpp(net)
        print("The voltage results are:")
        print(net.res_bus.vm_pu)
        print("The loading results are:")
        print(net.res_line.loading_percent)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

