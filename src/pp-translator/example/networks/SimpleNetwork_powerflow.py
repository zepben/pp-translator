# Basic example for extracting data from the ewb server and running power flow with pandapower
import asyncio
from zepben.evolve import connect_async, NetworkConsumerClient, NetworkService, Equipment, Feeder, EnergySource
from zepben.evolve import EnergySourcePhase, ConnectivityNode, ConductingEquipment, BaseVoltage, Terminal, Disconnector
from zepben.evolve import EquipmentContainer, AcLineSegment, Junction, EnergyConsumer, Breaker, PowerTransformer, Fuse, \
    Recloser
from zepben.evolve import basevoltage_to_cim, EnergyConsumerPhase

import pandas as pd
import pandapower as pp


def get_index(node_names, nodes):
    dfb = int(nodes[nodes['name'] == node_names].index[0])
    return dfb


async def main():
    async with connect_async(host="localhost", rpc_port=50052) as channel:
        service = NetworkService()
        client = NetworkConsumerClient(channel)
        result = (await client.get_feeder(service,'CPM3B3')).throw_on_error()
        print(service.get('CPM3B3'))
        net = pp.create_empty_network()
        pd.set_option('display.max_columns', None)
        # if(Feeder.mrid=="CPM3B3"):
        # for attr in dir(Feederp):
        # print("Feederp.%s = %r" % (attr, getattr(Feederp, attr)))
        MV = 11000 / 1000
        LV = 416 / 1000
        for Base in service.objects(BaseVoltage):
            print("BaseVoltages :" + Base.mrid)
        for Term in service.objects(ConnectivityNode):
            vn_kv = LV
            if Term.mrid == "SourceNode":
                vn_kv = MV
                print("-", Term.mrid, "-", Term._terminals[0].phases.name)
            print("-", Term.mrid, "-", Term._terminals[0].phases.name, "-", Term._terminals[1].phases.name)
            pp.create.create_bus(net, vn_kv=vn_kv, name=Term.mrid)
        a = net.bus
        print("Panda power buses are:")
        print(a)
        #        s=get_index("Bus 70",a)
        # dfb = int(a[a['name']=="Bus 70"].index[0])
        #        print(s)
        for PhaseGen in service.objects(EnergySourcePhase):
            print("Phases are:", PhaseGen.mrid, PhaseGen.name)

        for PhaseLoads in service.objects(EnergyConsumerPhase):
            print("Phases are:", PhaseLoads.mrid, PhaseLoads.name)

        for Load in service.objects(EnergyConsumer):
            Fases = Load._terminals[0].phases.name
            bus = get_index(Load._terminals[0].connectivity_node.mrid, a)
            p_mw = (Load.p / 1000000)
            q_mvar = (Load.q / 1000000)
            if Fases == "A":
                pp.create_asymmetric_load(net, name=Load.mrid, bus=bus, p_a_mw=p_mw, q_a_mvar=q_mvar, type='wye')
            elif Fases == "B":
                pp.create_asymmetric_load(net, name=Load.mrid, bus=bus, p_b_mw=p_mw, q_b_mvar=q_mvar, type='wye')
            elif Fases == "C":
                pp.create_asymmetric_load(net, name=Load.mrid, bus=bus, p_c_mw=p_mw, q_c_mvar=q_mvar, type='wye')
            elif Fases == "ABC" or Fases == "ABCN":
                pp.create_load(net, name=Load.mrid, bus=bus, p_mw=p_mw, q_mvar=q_mvar)

            # print("-",bus,"-",p_mw,q_mvar)
            # pp.create_load(net,name=Load.mrid,bus=bus,p_mw=p_mw,q_mvar=q_mvar)
            # print(Load.mrid)
        print("Pandapower Loads are:")
        print(net.load)
        print(net.asymmetric_load)

        for Feederp in service.objects(Feeder):
            Nodo_Cabecera = get_index(Feederp.normal_head_terminal.connectivity_node.mrid, a)
            print(Nodo_Cabecera)

        # External source
        for Ext in service.objects(EnergySource):
            bus = get_index(Ext._terminals[0].connectivity_node.mrid, a)
            if bus == Nodo_Cabecera:
                p_mw = Ext.p_max + 1
                #                slack=False
                #                if Ext.mrid == "EnergySource":
                #                slack=True
                pp.create_ext_grid(net, bus=bus, name=Ext.mrid, p_mw=p_mw, va_degree=0, s_sc_max_mva=1000,
                                   s_sc_min_mva=800, rx_max=0.1, r0x0_max=1, x0x_max=1, r0x0_min=0.1, x0x_min=1, )
                print("Pandapower external grids are:" + Ext.mrid, "|", bus, Ext.voltage_magnitude, p_mw)
        print(net.ext_grid)

        for Generators in service.objects(EnergySource):
            Fases = Generators._terminals[0].phases.name
            bus = get_index(Generators._terminals[0].connectivity_node.mrid, a)
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
                    pp.create_gen(net, name=Generators.mrid, bus=bus, p_mw=p_mw, q_mvar=q_mvar)
                # print("2")
                print("Pandapower generators are:" + Generators.mrid, "|", bus, Generators.voltage_magnitude, sn_mva, angle)
        print(net.asymmetric_sgen)

        for Transformer in service.objects(PowerTransformer):
            hv_bus = get_index(Transformer._terminals[0].connectivity_node.mrid, a)
            lv_bus = get_index(Transformer._terminals[1].connectivity_node.mrid, a)
            sn_mva = Transformer._power_transformer_ends[0].rated_s
            vn_hv_kv = MV
            vn_lv_kv = LV
            vk_percent = 2
            vkr_percent = 0.001
            pfe_kw = 0.001
            i0_percent = 0.1
            shift_degree = 330
            pp.create_transformer_from_parameters(net, name=Transformer.mrid, hv_bus=hv_bus, lv_bus=lv_bus,
                                                  sn_mva=sn_mva, vn_hv_kv=vn_hv_kv, vn_lv_kv=vn_lv_kv,
                                                  vkr_percent=vkr_percent, vk_percent=vk_percent, pfe_kw=pfe_kw,
                                                  i0_percent=i0_percent, shift_degree=shift_degree, vector_group="Dyn",
                                                  vk0_percent=4.01995, vkr0_percent=0.4, mag0_percent=100, mag0_rx=0,
                                                  si0_hv_partial=0.9)
            print("Pandapower transformers are:" + Transformer.mrid, "|", hv_bus, "|", lv_bus, "|", sn_mva)
        print(net.trafo)

        for Switch in service.objects(Breaker):
            bus = get_index(Switch._terminals[0].connectivity_node.mrid, a)
            element = get_index(Switch._terminals[1].connectivity_node.mrid, a)
            et = 'b'
            type = "CB"
            pp.create_switch(net, name=Switch.mrid, bus=bus, element=element, et=et, type=type)
            print("Pandapower Switches are:" + Switch.mrid, "|", Switch.in_service)
        print(net.switch)
        for Equip in service.objects(AcLineSegment):
            from_bus = get_index(Equip._terminals[0].connectivity_node.mrid, a)
            to_bus = get_index(Equip._terminals[1].connectivity_node.mrid, a)
            length_km = Equip.length / 1000
            ##            std_type=Equip.per_length_sequence_impedance.mrid
            # Se debe borrar el *1.000 están mal los valores de r y x
            r_ohm_per_km = (Equip.per_length_sequence_impedance.r) * (length_km)
            x_ohm_per_km = (Equip.per_length_sequence_impedance.x) * (length_km)
            c_nf_per_km = (Equip.per_length_sequence_impedance.bch) * (length_km)
            print(Equip.per_length_sequence_impedance.r, "|", length_km, "|",
                  Equip.per_length_sequence_impedance.r * length_km, "|", r_ohm_per_km)
            r0_ohm_per_km = (Equip.per_length_sequence_impedance.r0) * length_km
            x0_ohm_per_km = (Equip.per_length_sequence_impedance.x0) * length_km
            c0_nf_per_km = (Equip.per_length_sequence_impedance.x0) * length_km
            max_i_ka = 1000
            #            print(length_km)
            # if(Equip.mrid=="acls1"or Equip.mrid=="acls5" or Equip.mrid=="acls6"or Equip.mrid=="acls7" or Equip.mrid=="acls2" or Equip.mrid=="acls3" ):
            pp.create_line_from_parameters(net, name=Equip.mrid, from_bus=from_bus, to_bus=to_bus, length_km=length_km,
                                           r_ohm_per_km=r_ohm_per_km, x_ohm_per_km=x_ohm_per_km,
                                           c_nf_per_km=c_nf_per_km, max_i_ka=max_i_ka, r0_ohm_per_km=r0_ohm_per_km,
                                           x0_ohm_per_km=x0_ohm_per_km, c0_nf_per_km=c0_nf_per_km)
        print("PandaPower transmission lines are:")
        print(net.line)
        # pp.add_zero_impedance_parameters(net)
        pp.runpp_3ph(net)
        # pp.runpp(net)
        print("Power flow results are:")
        # print(net.res_bus)
        pd.set_option('display.max_columns', None)
        print(net.res_bus_3ph)
        # print(net.res)

        # print(net.res_line)
        # print(Switch.mrid,"|",bus,"|",element)
        # if(Switch.mrid=="Breaker"):
        # for attr in dir(net):
        #   print("net.%s = %r" % (attr, getattr(net, attr)))

        # for attr in dir(Transformer):
        # print("Transformer.%s = %r" % (attr, getattr(Transformer, attr)))


'''        for Ext in service.objects(EnergySource):
            bus=get_index(Ext._terminals[0].connectivity_node.mrid,a)
            p_mw=Ext.p_max+1
            slack=False
            if Ext.mrid == "EnergySource":
                slack=True
                pp.create_ext_grid(net,bus=bus,name=Ext.mrid,p_mw=p_mw,va_degree=0)
                print("Las redes externas son:" + Ext.mrid,"|",bus,Ext.voltage_magnitude,p_mw)
        print(net.ext_grid)

        for Generators in service.objects(EnergySource):
            bus=get_index(Generators._terminals[0].connectivity_node.mrid,a)
            try:
                p_mw=Generators.activePower
            except:
                p_mw=0.001
            sn_mva=Generators.p_max
            angle=Generators.voltage_angle
            slack=False
            if Generators.mrid == "EnergySource":
                slack=True
                #pp.create_ext_grid(net,bus=bus,name=Generators.mrid,p_wm=0.5,va_degree=0)
            else:
                pp.create_gen(net,bus=bus,name=Generators.mrid,p_mw=p_mw,sn_mva=sn_mva,slack=slack)
                #print("2")
                print("Los generadores son:" + Generators.mrid,"|",bus,Generators.voltage_magnitude,sn_mva,angle)
        print(net.gen) 

        for Load in service.objects(EnergyConsumer):
        Fases=Load._terminals[0].phases.name
            if Fases=="A":
                

            
            p_mw=(Load.p/1000000)
            q_mvar=(Load.q/1000000)
            #print("-",bus,"-",p_mw,q_mvar)
            pp.create_load(net,name=Load.mrid,bus=bus,p_mw=p_mw,q_mvar=q_mvar)
            #print(Load.mrid)
        print("las cargas en PandaPower son:")
        print(net.load)
       
'''

# for Term in service.objects(EnergyConsumer):
# print(Term._terminals[0].connectivity_node.mrid)
# if(Term.mrid=="EnergyConsumer1"):
# for attr in dir(Term):
# print("Term.%s = %r" % (attr, getattr(Term, attr)))
# print(dir(zepben.evolve))
# ok, falta el nivel de tensión
#        for Term in service.objects(Terminal):
#            print(Term.mrid,"|||", Term.name,Term.phases.name,Term.conducting_equipment.mrid,"|",type(Term.conducting_equipment))
#            pp.create.create_bus(net,vn_kv=13,name=Term.mrid)
#            print(Term.mrid,"|||", Term.name,Term.phases.name)
#            pp.create.create_bus(net,vn_kv=13,name=Term.mrid)
#           print(Term.mrid,"|||", Term.name,Term.phases.name,Term.conducting_equipment.mrid,"|",Term.conducting_equipment)
'''        for Term in service.objects(ConnectivityNode):
            pp.create.create_bus(net,vn_kv=13,name=Term.mrid)
        print(net.bus)
'''
'''        for Equip in service.objects(AcLineSegment):
            from_bus=Equip._terminals[0].connectivity_node.mrid
            to_bus=Equip._terminals[1].connectivity_node.mrid
            length_km=Equip.length
##            std_type=Equip.per_length_sequence_impedance.mrid
            r_ohm_per_km=Equip.per_length_sequence_impedance.r
            x_ohm_per_km=Equip.per_length_sequence_impedance.x
##           c_nf_per_km=
            r0_ohm_per_km=Equip.per_length_sequence_impedance.r0
            x0_ohm_per_km=Equip.per_length_sequence_impedance.x0
            max_i_ka=90
            if(Equip.mrid=="cable3604374"):
                print(Equip.mrid,"|",Equip.name,"|",from_bus,"|",to_bus,"|",length_km,"|",r_ohm_per_km,"|",x_ohm_per_km,"|",r0_ohm_per_km,"|",x0_ohm_per_km,"|",max_i_ka)
'''

#            print(Equip.mrid,"|",Equip.name,Equip._terminals[0].connectivity_node.mrid,"|",Equip._terminals[1].connectivity_node.mrid)
#            if(Equip.mrid=="cable3604374"):
#                for attr in dir(Equip):
#                    print("Equip.%s = %r" % (attr, getattr(Equip, attr)))
#        for Load in service.objects(EnergyConsumer):
#            print(Load.mrid,"|||", Load.name,"|",Load.Terminal.mrid)
#            for Term1  in service.objects(Terminal):
#                if Term1.conducting_equipment.mrid==Load.mrid:
#                    print(Load.mrid,"|||", Load.name,"|",Term1.mrid,"|",Term1.connectivity_node.mrid)
# pp.create.create
#        for LoadPhase in service.objects(EnergyConsumerPhase):
#            print(LoadPhase.mrid,"|||", LoadPhase.name)

#            if(Load.mrid=="service_point623150"):
#                for attr in dir(Load):
#                    print("Load.%s = %r" % (attr, getattr(Load, attr)))


#        for equip in service.objects(Equipment):
#            print(equip.mrid,"|", equip.name,"|", type(equip))
#        for equipCont in service.objects(EquipmentContainer):
#            print(equipCont.mrid,"|||", equipCont.name,type(equipCont))
#        for equipCont in service.objects(BaseVoltage):
#            print(equipCont.mrid,"|||", equipCont.nominalVoltage   )


#        for Load in service.objects(Terminal):

#   print(net.bus)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
