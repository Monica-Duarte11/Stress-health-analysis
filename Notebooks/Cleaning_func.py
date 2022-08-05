#!/usr/bin/env python
# coding: utf-8

# In[1]:


import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import glob


# In[2]:


def imp_and_apnd(folder):
    '''
    folder = str
    folder is a 
    Dado el nombre de una carpeta en la misma ubicación, importa cada uno de los archivos .xml del directorio y lo convierte en un
    DataFrame de Pandas, con los datos de todos los ficheros de ese directorio
    '''
    filenames =  glob.glob(f'{folder}/*.xml')
    record_list = []
    data2 = pd.DataFrame()
    count=0
    for filename in filenames:
        count +=1
        tree = ET.parse(filename)
        root = tree.getroot()
        record_list = [x.attrib for x in root.iter('Record')] 
        data = pd.DataFrame(record_list)
        data['Person']= count
        data2 = data2.append(data)
    data2.reset_index(inplace=True)

    return data2


# In[8]:


def date_cleaning (data, col):
    '''
    Dado el nombre del dataFrame pasamos las columnas de datos de fechas  tipo 'datetime'
    y la columna 'value' a tipo numérico conservando los valores nulos
    '''
    data[col]= data[col].apply(lambda row: str(row).split('+')[0])
    data[col] = pd.to_datetime(data[col], format="%Y-%m-%d %H:%M:%S", utc=True)
 
    return data


# In[ ]:


def date_2022 (data):
    '''
    Dado un dataframe con columna tipo DateTime 
    Seleccionamos solo aquellos regustros que correspondan al año 2022
    Filtramos y guardamos el nuevo dataframe
    '''
    data = data[data['creationDate'].dt.year == 2022]
    return data


# In[3]:


def type_cleaning(data, col):
    '''
    data = DataFrame
    Dado un dataFrame 
    Tomamos la columna 'type' con los tipos de variables que mide Apple Health en formato string
    Y limpiamos los valores
    '''
    data[col] = data[col].str.replace('HKQuantityTypeIdentifier', '')
    data[col] = data[col].str.replace('HKDataType', '')
    data[col] = data[col].str.replace('HKCategoryTypeIdentifier', '')
    data[col] = data[col].str.replace('HKCategory', '')
    return data


# In[4]:


def dropping_col (dataframe, col):
    '''
    Dado el nombre de un DataFrame y el nombre de una columna o lista de nombres (en formato string)
    Eliminamos la columna del data frame
    '''
    dataframe.drop(col, inplace = True, axis= 1)
    return dataframe


# In[5]:


def sleep_cleaning (data):
    '''
    data = DataFrame
    Dado un detaFrame, tomamos la columna 'type' y
    Eliminamos y guardamos los datos de sueño 
    '''
    sleep = data[data['type']== 'SleepAnalysis']
    data.drop(data[data['type']== 'SleepAnalysis'].index, inplace = True)
    mediciones_2 = data['type'].unique()
    return data, sleep


# In[6]:


def measured_var (dataframe, col):
    '''
    Dado el nombre de un DataFrame y el nombre de la columna en formato string guardamos en una variable 
    '''
    var_med = dataframe[col].unique()
    return var_med


# In[7]:


def to_num (dataframe,col):

    '''
    dataframe = DataFrame
    Dado un detaFrame y el nombre de una columna.
    Convertimos la columna a dato tipo numérico
    '''
    dataframe['value_2'] = dataframe.apply(lambda row: pd.to_numeric(row[col], errors = 'coerce'), axis = 1)
    no_num_val = dataframe[dataframe['value_2'].isnull()]['type'].unique()
    
    return dataframe, no_num_val

