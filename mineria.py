import pyshark
from cassandra.cluster import Cluster
import keyboard

#NECESARIO keyspace con nombre NoSQLMDAD, ejemplo:
#create keyspace NoSQLMDAD WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};

#Crear conexión a la base de datos
def setConnection(host, port, keyspace):
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace=keyspace)
    return session

#Captura de Wireshark
def startCapture(netAdapter):
    return pyshark.LiveCapture(netAdapter)


#Menú de instrucciones, se queda un par de segundos
print("---------------------------------")
print("Presiona P para pausar")
print("Presiona Esc para salir")
print("---------------------------------")

#Conexión a la base de datos
session = setConnection('127.0.0.1', port=9042, keyspace='nosqlmdad')
capture = startCapture('Wi-Fi')

#Creación de tablas
session.execute("CREATE TABLE IF NOT EXISTS packets (id double PRIMARY KEY, timestamp text, srcIp text, dstIp text, dstPort text, protocol text)")
session.execute("TRUNCATE packets")

#Preparación de sentencia
prepared = session.prepare("INSERT INTO packets (id, timestamp, srcIp, dstIp, dstPort, protocol) VALUES (?,?,?,?,?,?)")

#Contador de paquetes
serie = 0

#Captura de paquetes
for packet in capture.sniff_continuously():
    id = float(packet.number)
    timestamp = str(packet.sniff_time)

    #Comprobación capa IP
    if(hasattr(packet, 'ip')):
        srcIp = packet.ip.addr
        dstIp = packet.ip.dst
        protocol = packet.highest_layer
    
    #Comprobación puerto destino
    try:
        if(hasattr(packet[protocol], 'dstport')):
            dstPort = packet[protocol].dstport
        else:
            dstPort = "None"
    except KeyError:
        dstPort = "None"
        
    #Ejecución de sentencia    
    bound = prepared.bind((id, timestamp, srcIp, dstIp, dstPort, protocol))
    session.execute(bound)
    serie += 1
    print("Packet: ", id, " inserted, ", serie, " packets inserted")
        
    #Pausa y cierre
    if keyboard.is_pressed("p"):
        print("Capture paused, press enter to continue")
        keyboard.wait("enter")
    if(keyboard.is_pressed("esc")):
        print("Capture stopped")
        break

#Cerrar sesion cassandra
if (None != session):
    try:
        print("Closing Cassandra connection...")
        session.shutdown()
    except:
        print("There has been an error closing the connection")
        
print("Session closed")

#Fin de captura        
if (None != capture):
    try:
        print("Closing capture...")
        capture.close()
    except:
        print("There has been an error closing the capture")
        
print("Capture closed")