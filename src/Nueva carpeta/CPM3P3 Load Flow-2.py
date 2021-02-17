import asyncio
import sys
import getopt
import pandas as pd
import pandapower as pp
from zepben.evolve import connect_async, NetworkConsumerClient, NetworkService
from zepben.evolve import Equipment,BaseVoltage
from zepben.evolve import NetworkService, Feeder, EnergySource, EnergySourcePhase, ConnectivityNode, \
    ConductingEquipment, BaseVoltage, Terminal, EquipmentContainer, AcLineSegment, Junction, EnergyConsumer, \
    Disconnector, Breaker, PowerTransformer, Location,Fuse, Recloser, DiagramObjectPoint,PositionPoint, basevoltage_to_cim, EnergyConsumerPhase
import zepben.evolve
def conn_to_junction(Connectivity_Point):
    # TODO: Solve problem of connectivity when there are fuses with more than 2 terminals
    Node = Connectivity_Point
    for Terminal in Connectivity_Point._terminals:
        if (type(Terminal._conducting_equipment) == zepben.evolve.model.cim.iec61970.base.wires.connectors.Junction):
            Node = Terminal._conducting_equipment
    return Node




#Tramo=Terminal._conducting_equipment
#Origen=ConnectivityNode
#Visitados=[]
#Voltage=0
def Recorrer(Origen,Visitados):
    Voltage=0
    for Tramo in Origen._terminals:
        if Tramo._conducting_equipment.base_voltage!=None:
            Voltage= Tramo._conducting_equipment.base_voltage.nominal_voltage
            break
        if Tramo._conducting_equipment not in Visitados and Tramo._conducting_equipment.base_voltage==None:
            Visitados.append(Tramo._conducting_equipment)
            for Terminal in Tramo._conducting_equipment._terminals:
                if Terminal.connectivity_node!=Origen:
                    Recorrer(Terminal.connectivity_node,Visitados)
    return Voltage

def search_voltage(Connectivity_Point):
    # TODO: Solve problem of connectivity when there are fuses with more than 2 terminals
    Node = Connectivity_Point
    Voltage=None
    for Terminal in Connectivity_Point._terminals:
        print("EL terminal es", Terminal)
        if Voltage==None:
            if Terminal._conducting_equipment.base_voltage!=None:
                Voltage=Terminal._conducting_equipment.base_voltage.nominal_voltage
        #for Element in Terminal._conducting_equipment:
                print("El volta es",Voltage)


            #if Voltage==None:
                #print(Element.mrid,type(Element).__name__,Element.base_voltage.nominal_voltage)
            #Voltage=Element.base_voltage.nominal_voltage
            #print("Elvolt",Voltage)

    return Voltage


def create_bus(service, net):
    # TODO:Function to obtain BaseVoltage
    MV = 11000 / 1000
    LV = 11000 / 1000
    data = {'name': [], 'vn_kv': [],'X':[],'Y':[]}
    df = pd.DataFrame(data)

    for Term in service.objects(ConnectivityNode):
        #vn_kv = LV
        #if Term.mrid == "SourceNode":
            #vn_kv = MV
            #print("-", Term.mrid, "-", Term._terminals[0].phases.name)
        new_row = {'name': conn_to_junction(Term).mrid, 'vn_kv': search_voltage(Term)}
        Visitados=[]
        #new_row = {'name': conn_to_junction(Term).mrid, 'vn_kv': Recorrer(Term,Visitados)}
        df = df.append(new_row, ignore_index=True)
    df = pd.DataFrame.drop_duplicates(df)
    for idx in df.index:
        pp.create_bus(net, name=df.at[idx, "name"], vn_kv=df.at[idx, "vn_kv"])
    print(len(df))
    return df

def print_feeder_eq(service):
    datavolt = {'name': [], 'type_element': [],'basevol':[]}
    df = pd.DataFrame(datavolt)
    for equip in service.objects(Equipment):

        #print(equip.mrid, equip.name, type(equip).__name__, equip.get_base_voltage())
        if equip.get_base_voltage()!=None:
            print(equip.mrid,type(equip).__name__, equip.base_voltage.mrid, equip.base_voltage.nominal_voltage)
            new_row = {'name': equip.mrid, 'type_element': type(equip).__name__,'basevol':equip.base_voltage.mrid}
            df = df.append(new_row, ignore_index=True)
    return df

async def main(argv):
    rpc_port = 50052
    host = "localhost"
    try:
        opts, args = getopt.getopt(argv, "h:i:p:u:", ["mrid=", "port=", "host="])
    except getopt.GetoptError:
        print('get_feeder.py -i <feeder_mrid> -p <rpc_port> -u <host>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('get_feeder.py -p <rpc_port> -i <feeder_mrid>')
            sys.exit()
        elif opt in ("-i", "--mrid"):
            feeder_mrid = arg
        elif opt in ("-p", "--port"):
            rpc_port = arg
        elif opt in ("-u", "--host"):
            host = arg
    async with connect_async(host=host, rpc_port=rpc_port) as channel:
        client = NetworkConsumerClient(channel)
        result = (await client.retrieve_network()).throw_on_error()
        service = result.result.network_service
        p=print_feeder_eq(service)
        #print(p)
        bv_11kv = BaseVoltage(mrid='11kv', nominal_voltage=11000, name='11kV')
        bv_045kv = BaseVoltage(mrid='0.45kv', nominal_voltage=415, name='0.45kV')
        service = NetworkService()
        client = NetworkConsumerClient(channel)
        result = (await client.get_feeder(service, 'CPM3B3')).throw_on_error()
        print(service.get('CPM3B3'))
        net = pp.create_empty_network()
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        print("el resultado es")
        for idx in p.index:
            for Equip in service.objects(Equipment):
                if Equip.mrid==p.at[idx,"name"] and type(Equip).__name__==p.at[idx,"type_element"]:
                    if p.at[idx,"basevol"]=="base-voltage-11000":
                        Equip.base_voltage=bv_11kv
                    if p.at[idx,"basevol"]=="base-voltage-415":
                        Equip.base_voltage=bv_045kv
                    #print(Equip.mrid,type(Equip).__name__,Equip.base_voltage.nominal_voltage)

        create_bus(service, net)
        a = net.bus
        print("The Results are")
        print(a)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(sys.argv[1:]))