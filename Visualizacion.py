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


#Conexión a la base de datos
session = setConnection('127.0.0.1', port=9042, keyspace='nosqlmdad')

print("----------------------------------------")

while(True):
    
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
    
    #fig = plt.figure(dpi=200)
    fig = plt.figure(figsize=(20, 12), dpi=85)
    
    #Muestra las gráficas
    ax1 = plt.subplot2grid((2,2),(0,0))
    plt.barh(df['dstip'].unique()[:10], df['dstip'].value_counts()[:10])
    plt.title('Common destination ips')
    
    ax1 = plt.subplot2grid((2,2),(0,1))
    colors=['#ff0000', '#00ff00', '#0000ff', '#ffff00', '#00ffff', '#ff00ff', '#000000', '#a2aa52', '#ffa500', '#a52a2a']
    plt.barh(df['srcip'].unique()[:10], df['srcip'].value_counts()[:10], color=colors)
    plt.title('Common source ips')
 
    ax1 = plt.subplot2grid((2,2),(1,0))
    plt.pie(df['dstport'].value_counts()[:10], shadow = True)
    plt.legend(title = "Common destination ports:", bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0, labels=df['dstport'].unique()[:10])   
    plt.title('Common destination ports')

    ax1 = plt.subplot2grid((2,2),(1,1))
    plt.pie(df['protocol'].value_counts()[:10], shadow = True)
    plt.legend(title = "Common protocols:", bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0, labels=df['protocol'].unique()[:10])  
    plt.title('Common protocols')
    
    plt.show()
        
print("----------------------------------------")

print("Closing Cassandra connection...")

if (None != session):
    try:
        session.shutdown()
    except:
        print("There has been an error closing the connection")
        
print("Session closed")