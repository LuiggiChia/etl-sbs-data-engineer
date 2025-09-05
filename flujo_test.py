import warnings
warnings.filterwarnings('ignore')
from datetime import date
from datetime import datetime, timedelta
import os
import pandas as pd

dic = {
    'var_1':['BANCO','B-2358'],
    'var_2':['FINANCIERA','B-3241']
}
columnas = ["Empresa","Departamento","Provincia","Distrito","Codigo_de_Oficina","Depositos_a_la_Vista_MN","Depositos_a_la_Vista_ME",
            "Depositos_a_la_Vista_Total","Depositos_de_Ahorro_MN","Depositos_de_Ahorro_ME","Depositos_de_Ahorro_Total",
            "Depositos_a_Plazo_MN","Depositos_a_Plazo_ME","Depositos_a_Plazo_Total","Total_Depositos","Creditos_Directos_MN",
            "Creditos_Directos_ME","Creditos_Directos_Total"]

fecha_de_ejecucion = datetime.now().strftime("%Y%m%d-%H%M%S")
df_total = pd.DataFrame()

ruta = os.path.join(os.getcwd(),"data")
lst_data = [f for f in os.listdir(ruta)]

def procesar_archivo(var_key,dic,lst_data,ruta,fecha_de_ejecucion):
    archivo = next((x for x in lst_data if dic[var_key][1] in x),None)
    if archivo is None:
        raise FileNotFoundError(f"No se encontró el archivo {dic[var_key][1]}")
    df = pd.read_excel(os.path.join(ruta,archivo))
    fecha_cierre = df.iloc[1,0].strftime("%Y%m")
    df = pd.read_excel(os.path.join(ruta,archivo),skiprows=6)
    df.columns = columnas
    df['Entidad'] = dic[var_key][0]
    df[df.select_dtypes(include="number").columns] = df.select_dtypes(include="number").round(2)
    df = df.fillna(method="ffill")
    df['FechaCierre'] = fecha_cierre
    df['FechaProceso'] = fecha_de_ejecucion
    mask = df['Empresa'].str.contains("Total general",case=False,na=False)
    idx = mask.idxmax() if mask.any() else len(df)
    df = df[:idx]
    return df

for var in ['var_1','var_2']:
    df_total = pd.concat([df_total,procesar_archivo(var,dic,lst_data,ruta,fecha_de_ejecucion)],ignore_index=True)

df_total.reset_index(drop=True, inplace=True)
df_total.to_parquet("Data.parquet", index=False)