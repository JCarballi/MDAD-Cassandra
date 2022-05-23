from os import environ
from cassandra.cluster import Cluster
from matplotlib import pyplot as plt
import pandas as pd

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

# Draw Plot
def plot_df(df, x, y, title="", xlabel='Date', ylabel='Layer', dpi=100):
    plt.figure(figsize=(16,5), dpi=dpi)
    plt.plot(x, y, color='tab:red')
    plt.gca().set(title=title, xlabel=xlabel, ylabel=ylabel)
    plt.xticks([])
    plt.show()


#Conexión a la base de datos
session = setConnection('127.0.0.1', port=9042, keyspace='nosqlmdad')
results = session.execute("SELECT timestamp, protocol FROM packets")


suppress_qt_warnings()

#Contador y array de resultados
total = 0
resultArray = []

if([] == results.current_rows):
    print("No hay datos en la base de datos")
    
#Creación de array de resultados
for row in results:
    resultArray.append(row)
    total+=1
    
#Creación del DataFrame
data = pd.DataFrame(resultArray)
data = data.sort_values(by=['timestamp'])

print("Total: ", total)

plot_df(data, x=data.timestamp, y=data.protocol, title='Layer vs Time')   

session.shutdown()
