from os import environ
from cassandra.cluster import Cluster
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import keyboard

#Quitar warnings Matplotlib
def suppress_qt_warnings():
    environ["QT_DEVICE_PIXEL_RATIO"] = "0"
    environ["QT_AUTO_SCREEN_SCALE_FACTOR"] = "1"
    environ["QT_SCREEN_SCALE_FACTORS"] = "1"
    environ["QT_SCALE_FACTOR"] = "1"

#Crear conexión a la base de datos
def setConnection(host, port, keyspace):
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace=keyspace)
    return session

#Los 10 origenes y destinos más frecuentes
def moreFrecuentDstIps(results):
    print("More frecuent destination IPs:")
    print(results.value_counts()[:10].sort_values(ascending=False).to_string())
    
#Los 10 origenes y destinos más frecuentes
def moreFrecuentSrcIps(results):
    print("More frecuent source IPs:")
    print(results.value_counts()[:10].sort_values(ascending=False).to_string())
    
#Los 10 origenes y destinos más frecuentes
def moreFrecuentDstPorts(results):
    print("More frecuent destination ports:")
    print(results.value_counts()[:10].sort_values(ascending=False).to_string())
    
#Los 10 origenes y destinos más frecuentes
def moreFrecuentProtocols(results):
    print("More frecuent protocols:")
    print(results.value_counts()[:10].sort_values(ascending=False).to_string())

#Conexión a la base de datos
session = setConnection('127.0.0.1', port=9042, keyspace='nosqlmdad')

#Menú de opciones
print("Cada cuánto desea actualizar:")
print("1. Manual")
print("2. 1 segundo")
print("3. 5 segundos")

key = input()

print("----------------------------------------")

suppress_qt_warnings()

while(True):
    
    #Menú de pause
    if keyboard.is_pressed("p"):
        print("Cada cuánto desea actualizar:")
        print("1. Manual")
        print("2. 1 segundo")
        print("3. 5 segundos")
        print("4. Salir")
        
        key = input()
        if(key == "4"):
            break
    
    #Contador y array de resultados
    total = 0
    resultArray = []
    
    results = session.execute("SELECT * FROM packets")
    
    if([] == results.current_rows):
        print("No hay datos en la base de datos")
        break
    
    #Creación de array de resultados
    for row in results:
        total += 1
        resultArray.append(row)
    
    #Creación del DataFrame
    df = pd.DataFrame(resultArray)
    
    #Impresión de resultados por consola
    print("-------------------RESULTS-------------------")
    print("Total: ", total)
    moreFrecuentDstIps(df['dstip'])
    moreFrecuentSrcIps(df['srcip'])
    moreFrecuentDstPorts(df['dstport'])
    moreFrecuentProtocols(df['protocol'])
    print("----------------END OF RESULTS----------------")
    
    
    #fig = plt.figure(dpi=200)
    fig = plt.figure(figsize=(14, 10), dpi=90)
    
    #Muestra las gráficas
    ax1 = plt.subplot2grid((2,2),(0,0))
    plt.pie(df['dstip'].value_counts()[:10], shadow = True)
    plt.legend(title = "Common destination ips:", bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0, labels=df['dstip'].unique()[:10])
    plt.title('Common destination ips')
    
    ax1 = plt.subplot2grid((2,2),(0,1))
    plt.pie(df['srcip'].value_counts()[:10], shadow = True)
    plt.legend(title = "Common source ips:", bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0, labels=df['srcip'].unique()[:10])
    plt.title('Common source ips')
 
    ax1 = plt.subplot2grid((2,2),(1,0))
    plt.pie(df['dstport'].value_counts()[:10], shadow = True)
    plt.legend(title = "Common destination ports:", bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0, labels=df['dstport'].unique()[:10])   
    plt.title('Common destination ports')

    ax1 = plt.subplot2grid((2,2),(1,1))
    plt.pie(df['protocol'].value_counts()[:10], shadow = True)
    plt.legend(title = "Common protocols:", bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0, labels=df['protocol'].unique()[:10])  
    plt.title('Common protocols')
    
    #Menú de opciones, entra en cada uno de los if según la opción elegida
    if(key == "1"):
        plt.show()
    elif(key == "2"):
        plt.show(block=False)
        plt.pause(1)
        plt.close()
    elif(key == "3"):
        plt.show(block=False)
        plt.pause(5)
        plt.close()
    else:
        plt.show()
        
print("----------------------------------------")

print("Closing Cassandra connection...")

if (None != session):
    try:
        session.shutdown()
    except:
        print("There has been an error closing the connection")
        
print("Session closed")