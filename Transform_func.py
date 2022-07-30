#!/usr/bin/env python
# coding: utf-8



import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import glob
from datetime import date, datetime



def type_use (data):
    '''
    Dado  un DataFrame  a partir de los datos de Apple Health,
    tomamos la column type y seleccionamos solo las variables que nos interesan
    
    '''
    data = data[data['type'].isin(['StepCount', 'BodyMass', 'HeartRate', 'DistanceCycling', 'ActiveEnergyBurned', 'DistanceWalkingRunning', 'RestingHeartRate', 'DistanceSwimming', 'DistanceDownhillSnowSports', 'RestingHeartRate', 'HeartRateVariabilitySDNN', 'RespiratoryRate', 'MindfulSession'])]
    
    return data




def cols_date (data):
    
    '''
    data = Datframe
    Dado un dataframe a partir de los datos de Apple Health,
    creamos columnas identificativas para fecha, año, mes, día y día de la semana
    '''
    data['Date']= pd.to_datetime(data['endDate'].dt.date)
    data['Time'] = pd.to_datetime(data['endDate'].dt.time, format='%H:%M:%S')    
    data['Weekday']= data['endDate'].dt.weekday
    
    return data




def cols_hour (data):
    '''
    data = Datframe
    Dado un dataframe a partir de los datos de Apple Health,
    creamos columnas identificativas para tiempo, hora, minutos y segundos  
    '''
    data['Time'] = data['endDate'].dt.time
    data['Hour'] = data['endDate'].dt.hour
    data['Minutes'] = data['endDate'].dt.minute
    data['Seconds'] = data['endDate'].dt.second
    return data




def null_clean (data, col):
    '''
    col = value_2
    '''
    null_type = data[data[col].isnull()]['type'].unique()
    for i in null_type:
        data.drop(data[data['type']== i].index, inplace = True)
    print (null_type)
    return data




def zero_count (data, col):
    ''' 
    data = Dataframe
    Dado un dataframe a partir de los datos de Apple Health,
    Calculamos los registros con valores igual a 0 para cada tipo de variable en la columna "type"
    Resultado se muestra como una lista 
    '''
    
    valor_0 = data[data['value_2'] == 0.0]['type'].unique()
    dic_zero = {}
    for i in range(len(valor_0)):
        fil_0 = data[(data['value_2'] == 0.0) & (data['type'] == valor_0[i])].shape[0]
        dic_zero[valor_0[i]] = fil_0

    return dic_zero




def unit_df(data):
    '''
    Dado un dataframe creamos un nuevo dataframe que extraiga las unidades por cada variable 
    '''
    
    units = data['Unit'].unique()
    df_unit = pd. DataFrame(columns = ['Variable', 'Units'])

    for i in units:
        unit_per_var= data[data['Unit'] == i]['Type'].unique()

        new_row= {'Variable':unit_per_var,
                 'Units': i}
        df_unit = df_unit.append(new_row, ignore_index=True)
    return df_unit



def days_count (data):
    '''
    Dado un Dataframe 
    Creamos un nuevo dataframe con el rango de fecha que tiene el dataframe por cada persona en el estudio
    '''
    df = pd. DataFrame()
    
    for i in data['Person'].unique():
        Date_ord= data[data['Person']== i]['Date'].sort_values().reset_index()
        first_date = Date_ord.loc[0, 'Date']
        last_val= len(Date_ord)-1
        last_date = Date_ord.loc[last_val, 'Date']
        delta = last_date - first_date
        
        new_row= {'Person': i, 
        'Count_days': delta,
        'first_day' : first_date,
        'Last_day': last_date}
        df = df.append(new_row, ignore_index=True)
        
    return df



def count_for_per(dataframe1, dataframe2):
    '''
    Dados 2 dataframe 
    Del primer dataframe calculamoscuantos días de datos tenemos por cada persona y por cada variable
    Y añadimos los cálculos de los días al segundo dataframe
    '''
    
    mediciones_2= dataframe1['Type'].unique()

    for i in dataframe1['Person'].unique():
        print(i)
        for e in mediciones_2:
            try:
                Date_ord= dataframe1[(dataframe1['Person']== i) & (dataframe1['Type'] == e)]['Date'].sort_values().reset_index(drop=True)
                first_date = Date_ord.loc[0]
                last_val= len(Date_ord)-1
                last_date = Date_ord.loc[last_val]
                delta = last_date - first_date
                print('días medidos dentro de', e, ': ',delta.days)
                dataframe2.loc[i-1,e] = delta.days
            except KeyError as e:
                pass


def comparation (data):
    '''
    dado un Dataframe
    Creamos un nuevo Datframe sacando medidas estadísticas de cada variable para cada persona del df
    '''
    
    df_compare = pd. DataFrame()

    for i in data['Person'].unique():
            Stp= data[(data['Person']== i) & (data['Type'] == 'StepCount')].groupby(['Date', 'Person']).agg({'Values':'sum'}).rename(columns = {"Values": "StepCount"})
            HrtRate = data[(data['Person']== i) & (data['Type'] == 'HeartRate')].groupby(['Date', 'Person']).agg({'Values':'mean'}).rename(columns = {"Values": "HeartRate"})
            BodyMss= data[(data['Person']== i) & (data['Type'] == 'BodyMass')].groupby(['Date', 'Person']).agg({'Values':'mean'}).rename(columns = {"Values": "BodyMass"})
            Energybrn =data[(data['Person']== i) & (data['Type'] == 'ActiveEnergyBurned')].groupby(['Date', 'Person']).agg({'Values':'sum'}).rename(columns = {"Values": "ActiveEnergyBurned"})
            Distance = data[(data['Person']== i) & (data['Type'] == 'DistanceWalkingRunning')].groupby(['Date', 'Person']).agg({'Values':'sum'}).rename(columns = {"Values": "Distance"})

            out1 = pd.merge(HrtRate, Stp, on = ['Date', 'Person'], how="outer")   
            out2 = pd.merge(Distance, Energybrn, on = ['Date', 'Person'], how="outer")
            out3=pd.merge(out1,out2,on = ['Date', 'Person'], how="outer")
            out4= pd.merge(out3, BodyMss, on = ['Date', 'Person'], how="outer")
            
            df_compare= df_compare.append(out4)
    df_compare.reset_index(inplace= True)
        
    return df_compare



def sleep_compare(data, col):
    '''
    data = DataFrame
    col = columna con la duración del sueño en segundos
    Dado un dataframe y el nombre de la columna
    Creamos un nuevo dataframe que contenga la duración del sueño por persona
    '''
    df_compare = pd. DataFrame()

    for i in data['Person'].unique():
        asleep = data[(data['Person']== i) & (data['value'] == 'ValueSleepAnalysisAsleep')].groupby(by=[data['Person'], data['startDate'].dt.date]).agg({col:'sum'}).rename(columns = {col: 'Time_Asleep'})/3600

        in_bed= data[(data['Person']== i) & (data['value'] == 'ValueSleepAnalysisAsleep')].groupby(by=[data['Person'], data['startDate'].dt.date]).agg({col:'sum'}).rename(columns = {col: 'Time_in_bed'})/3600


        out1 = pd.merge(asleep, in_bed, on = ['startDate', 'Person'], how="outer")   
        df_compare= df_compare.append(out1)

    df_compare = df_compare.sort_values('startDate').reset_index()
    df_compare.rename(columns={'startDate':'Date'}, inplace = True) 
    df_compare['Date'] = df_compare['Date'].apply(lambda x: datetime.strptime(str(x),'%Y-%m-%d'))
    return df_compare



def resume(data):
    '''
    data = DataFrame
    Dado un dataframe creamos uno nuevo con los valores de las variables "StepCount, HeartRateVariabilitySDNN, RespiratoryRate, RestingHeartRate" 
    para cada persona en el dataset
    '''
    df_compare = pd. DataFrame()

    Steps =  data[data['Type'] == 'StepCount'].groupby(by='Date').agg({'Values':'sum', 'Weekday': 'mean'}).rename(columns = {"Values": "StepCount"})
    HRV = data[data['Type'] == 'HeartRateVariabilitySDNN'].groupby(by='Date').mean().rename(columns = {"Values": "HRV"})
    Resp = data[data['Type'] == 'RespiratoryRate'].groupby(by='Date').mean().rename(columns = {"Values": "Resp_Rate"})
    RHR = data[data['Type'] == 'RestingHeartRate'].groupby(by='Date').mean().rename(columns = {"Values": "Rest_HR"})

    out1 = pd.merge(Steps, Resp, left_index=True,right_index=True, how="outer")   
    out2 = pd.merge(HRV, RHR, left_index=True,right_index=True, how="outer")
    out3 = pd.merge(out1, out2, left_index=True,right_index=True, how="outer")
    out3.reset_index(inplace= True)
    out3.rename(columns= {'Weekday_x_x' : 'Weekday'}, inplace = True)
    out3 = out3[out3['Date'].dt.year == 2022].drop(['Person','Weekday_y_x','Person_x','Weekday_x_y', 'Person_y', 'Weekday_y_y'], axis=1)


    return out3



def diferencia(d1, d2):
    '''
    Dados dos valores de fecha en formato "%Y-%m-%d %H:%M:%S +0000"
    calculamos la diferencia de tiempo entre cambos valores
    '''
    d1= str(d1).split('+')[0]
    d2= str(d2).split('+')[0]
    d1= str(d1).split('-0600')[0]
    d2= str(d2).split('-0600')[0]
    d1= str(d1).split('-0300')[0]
    d2= str(d2).split('-0300')[0]
    d1 = datetime.strptime(d1,"%Y-%m-%d %H:%M:%S")
    d2 = datetime.strptime(d2,"%Y-%m-%d %H:%M:%S")
    return (d2 - d1).seconds