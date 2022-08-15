#!/usr/bin/python3
import requests
import os
import logging 


import pandas as pd
import numpy as np
from datetime import datetime

from decouple import config
from sqlalchemy import create_engine, event, Date, Integer, String

logging.basicConfig(filename="info.log", level=logging.INFO,
                    format="%(asctime)s::%(levelname)s::%(message)s")

#Direcciones de archivos 
url_museos = config('url_museos')
url_cines = config('url_cines')
url_bibliotecas = config('url_bibliotecas')


#Fechas actualizadas
now = datetime.now()
day=str(now.day)
month=str(now.month)
year=str(now.year)

#  Descargo la base de datos de MUSEOS
req_museos = requests.get(url_museos)
logging.info("Datos de Museos descargados")
os.makedirs('museos/'+year+'-'+str(now.strftime("%B"))+'/',exist_ok=True)
museos = open ('museos/'+year+'-'+str(now.strftime("%B"))+'/museos-'+day+'-'+month+'-'+year+'.csv','wb').write(req_museos.content)
logging.info("Archivos creados")

# Descargo la base de datos de SALAS DE CINE
req_cines = requests.get(url_cines)
logging.info("Datos de Salas de Cine descargados")
os.makedirs('cines/'+year+'-'+str(now.strftime("%B"))+'/',exist_ok=True)
cines = open ('cines/'+year+'-'+str(now.strftime("%B"))+'/cines-'+day+'-'+month+'-'+year+'.csv','wb').write(req_cines.content)
logging.info("Archivos creados")

# Descargo la base de datos de BIBLIOTECAS
req_bibliotecas = requests.get(url_bibliotecas)
logging.info("Datos de Bibliotecas descargados")
os.makedirs('bibliotecas/'+year+'-'+str(now.strftime("%B"))+'/',exist_ok=True)
bibliotecas = open ('bibliotecas/'+year+'-'+str(now.strftime("%B"))+'/bibliotecas-'+day+'-'+month+'-'+year+'.csv','wb').write(req_bibliotecas.content)
logging.info("Archivos creados")


##   Normalización datos de Museo  ##
df_museos=pd.read_csv('museos/'+year+'-'+str(now.strftime("%B"))+'/museos-'+day+'-'+month+'-'+year+'.csv', header=0 )

df_museos= df_museos.rename(columns = {'Cod_Loc':'cod_localidad','IdProvincia':'id_provincia'
                                      ,'IdDepartamento':'id_departamento','Observaciones':'remove1'
                                      ,'categoria':'categoría','subcategoria':'remove2'
                                      ,'provincia':'provincia','localidad':'localidad'
                                      ,'nombre':'nombre','direccion':'domicilio'
                                      ,'piso':'remove3','CP':'código postal'
                                      ,'cod_area':'remove4','telefono':'número de teléfono'
                                      ,'Mail':'mail','Web':'web'
                                      ,'Latitud':'remove5','Longitud':'remove6'
                                      ,'TipoLatitudLongitud':'remove7','Info_adicional':'remove8'
                                      ,'fuente':'fuente','jurisdiccion':'remove9'
                                      ,'año_inauguracion':'remove10','actualizacion':'remove11'})
for i in range(1,12):
    df_museos = df_museos.drop(columns={"remove"+str(i)})

logging.info("Normalización Museos")




##  Normalización datos de Salas de Cine  ##
df_cines = pd.read_csv('cines/'+year+'-'+str(now.strftime("%B"))+'/cines-'+day+'-'+month+'-'+year+'.csv', header=0 )

df_cines = df_cines.rename(columns = {'Cod_Loc':'cod_localidad','IdProvincia':'id_provincia'
                                      ,'IdDepartamento':'id_departamento','Observaciones':'remove1'
                                      ,'Categoría':'categoría','Provincia':'provincia'
                                      ,'Departamento':'remove2','Localidad':'localidad'
                                      ,'Nombre':'nombre','Dirección':'domicilio'
                                      ,'Piso':'remove3','CP':'código postal'
                                      ,'cod_area':'remove4','Teléfono':'número de teléfono'
                                      ,'Mail':'mail','Web':'web'
                                      ,'Información adicional':'remove5','Latitud':'remove6'
                                      ,'Longitud':'remove7','TipoLatitudLongitud':'remove8'
                                      ,'Fuente':'fuente','tipo_gestion':'remove9'
                                      ,'Pantallas':'pantallas','Butacas':'butacas'
                                      ,'espacio_INCAA':'espacio incaa','año_actualizacion':'remove10'})
for i in range(1,11):
    df_cines = df_cines.drop(columns={"remove"+str(i)})


logging.info("Normalización Salas de Cines")






##   Normalización datos de Bibliotecas  ##
df_bibliotecas = pd.read_csv('bibliotecas/'+year+'-'+str(now.strftime("%B"))+'/bibliotecas-'+day+'-'+month+'-'+year+'.csv', header=0 )

df_bibliotecas = df_bibliotecas.rename(columns = {'Cod_Loc':'cod_localidad','IdProvincia':'id_provincia'
                                      ,'IdDepartamento':'id_departamento','Observacion':'remove1'
                                      ,'Categoría':'categoría','Subcategoria':'remove2'
                                      ,'Provincia':'provincia','Departamento':'remove3'
                                      ,'Localidad':'localidad','Nombre':'nombre'
                                      ,'Domicilio':'domicilio','Piso':'remove4'
                                      ,'CP':'código postal','Cod_tel':'remove5'
                                      ,'Teléfono':'número de teléfono','Mail':'mail'
                                      ,'Web':'web','Información adicional':'remove6'
                                      ,'Latitud':'remove7','Longitud':'remove8'
                                      ,'TipoLatitudLongitud':'remove9','Fuente':'fuente'
                                      ,'Tipo_gestion':'remove10','año_inicio':'remove11'
                                      ,'Año_actualizacion':'remove12'})
for i in range(1,13):
    df_bibliotecas = df_bibliotecas.drop(columns={"remove"+str(i)})

logging.info("Normalización Bibliotecas")





## Creo la tabla normalizada con datos de Museos, Cines y Bibliotecas ##
principal_mcb=pd.DataFrame(columns=["cod_localidad", "id_provincia", "id_departamento"
                      , "categoría", "provincia", "localidad", "nombre"
                      , "domicilio", "código postal", "número de teléfono"
                      , "mail", "web"])

principal_mcb=principal_mcb.append([df_museos, df_cines, df_bibliotecas], ignore_index=True)
principal_mcb_fuente = principal_mcb
principal_mcb=principal_mcb.drop(columns={"fuente","pantallas","butacas","espacio incaa"})
principal_mcb['fecha de actualización'] = pd.Timestamp.now()

principal_mcb["provincia"]=principal_mcb["provincia"].replace(["Santa Fe"], "Santa Fé")
principal_mcb["provincia"]=principal_mcb["provincia"].replace(["Tierra del Fuego"], "Tierra del Fuego, Antártida e Islas del Atlántico Sur")
principal_mcb["provincia"]=principal_mcb["provincia"].replace(["Neuquén\xa0"], "Neuquén")



principal_mcb.to_csv("principal_mcb.csv",encoding='UTF-8')
logging.info("Tabla principal con datos de Museos, Salas de Cine y Bibliotecas Creadas")


## Creo la tabla de fuentes ##
tabla_fuente=principal_mcb_fuente["fuente"].value_counts().rename_axis("fuente").reset_index(name='registros por fuente')
tabla_fuente['fecha de actualización'] = pd.Timestamp.now()
tabla_fuente.to_csv("registros_por_fuente.csv",encoding='UTF-8')
logging.info("Tabla de registros por Fuente Creada")


## Creo Tabla registros por provincia y categoría ##
tabla_prov_cat=principal_mcb.pivot_table(values="nombre", index="provincia", columns="categoría",aggfunc="count", margins=True,margins_name="Registros totales por provincia")
                                         
tabla_prov_cat['fecha de actualización'] = pd.Timestamp.now()
tabla_prov_cat.to_csv("registros_prov_categoria.csv",encoding='UTF-8')
logging.info("Tabla de registros por Provincia y Categoría Creada")


# Creo la tabla de información de cines provincia, pantallas, butacas y espacios INCAA
df_cines["espacio incaa"]=df_cines["espacio incaa"].replace(["Si","SI","si"], int(1))
df_cines["espacio incaa"]=df_cines["espacio incaa"].replace([np.NaN], int(0))
df_cines["espacio incaa"]=df_cines["espacio incaa"].astype("int")

prov_info_cines=df_cines.groupby('provincia').sum()
prov_info_cines=prov_info_cines.drop(columns={"cod_localidad","id_provincia","id_departamento","código postal"})
prov_info_cines['fecha de actualización'] = pd.Timestamp.now()
prov_info_cines.to_csv("datos_cines_provincias.csv", encoding='UTF-8')
logging.info("Tabla de datos de Cines Creada")



## POSTGRESQL ##
db = create_engine('postgresql+psycopg2://'+config("MYSQL_USER")+':'+config("MYSQL_PWD")+'@'+config("MYSQL_HOST")+'/'+config("MYSQL_DBNAME"))
logging.info("Base de datos Creada")

## Importo los archivos ##
PRINCIPAL_MCB=pd.read_csv("principal_mcb.csv")
REGISTROS_FUENTE=pd.read_csv("registros_por_fuente.csv")
REGISTROS_PROV_CAT=pd.read_csv("registros_prov_categoria.csv")
CINES_PROV=pd.read_csv("datos_cines_provincias.csv")


PRINCIPAL_MCB.to_sql('museos_cines_bibliotacas', con=db, if_exists='replace')
REGISTROS_FUENTE.to_sql('registros_fuentes', con=db, if_exists='replace')
REGISTROS_PROV_CAT.to_sql('registros_prov_cat', con=db, if_exists='replace')
CINES_PROV.to_sql('cines_provincias', con=db, if_exists='replace')
logging.info("Archivos agregados a la Base de Datos")
