#Задействованные модули
import pandas as pd
import os
import numpy as np
import sys
import requests
import zipfile
# import matplotlib.pyplot as plt
# import statsmodels.formula.api as smf

#==========================================================================================
waves_dict={1994:[5,'A'],
           1995:[6,'B'],
           1996:[7,'C'],
           1998:[8,'D'],
           2000:[9,'E'],
           2001:[10,'F'],
           2002:[11,'G'],
           2003:[12,'H'],
           2004:[13,'I'],
           2005:[14,'J'],
           2006:[15,'K'],
           2007:[16,'L'],
           2008:[17,'M'],
           2009:[18,'N'],
           2010:[19,'O'],
           2011:[20,'P'],
           2012:[21,'Q'],
           2013:[22,'R'],
           2014:[23,'S'],
           2015:[24,'T'],
           2016:[25,'U'],
           2017:[26,'V'],
           2018:[27,'W'],
           2019:[28,'X'],
           2020:[29,'Y'],
           2021:[30,'Z'] 
           }
#==========================================================================================
def download_rlms_db(year='all', path=os.getcwd(), var='all', del_zip=True):
    """
    Скачивает в выбранный путь (или в текущую директорию) базу данных RLMS. 
    
    Параметры
    ---------
    year : integer, list, string, optional
        (default 'all')
        Год волны исследования, или список лет исследования.
        Если 'all', то скачивают базу данных полностью. 
    path : string, optional
        (default 'active_dir')
        Директория загрузки базы данных исследования.
    del_zip : bool, optional
        (default True)
        Если True, то удаляет скачанный архив после разархивирования. 
        Если False, то не удаляет архив после разархивирования. 
    var : string, optional
        (default 'all')
        Если 'hh', то загружает только данные домохозяйств. 
        Если 'ind', то загружает только данные индивидов. 
    verbose : bool, optional
        (default False)
        Если True, то отображает прогресс работы функции. 
        Если False, то не отображает прогресс работы функции. 
    """
    
    
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
    link = 'https://disk.yandex.ru/d/q8w4Od9tPqPl5Q'  # Сюда вписываете вашу ссылку
    
    # Получаем загрузочную ссылку
    final_url = base_url + 'public_key=' + link
    response = requests.get(final_url)
    download_url = response.json()['href']
    
    if path==os.getcwd():
        path=''
    
    download_response = requests.get(download_url)
    with open(f'{path}archive_rlms.zip', 'wb') as f:   # Здесь укажите нужный путь к файлу
        f.write(download_response.content)
        
    with zipfile.ZipFile(f'{path}archive_rlms.zip', 'r') as zip_ref:
        zip_ref.extractall()
    
    if del_zip==True:
        os.remove('archive_rlms.zip')
#==========================================================================================
"""
ЧТЕНИЕ ИНДИВИДУАЛЬНЫХ ДАННЫХ
"""
#==========================================================================================
def read_wave_ind(year,path=os.getcwd()+'\\RLMS_db'):
    """
    Загрузка выбранной волны инидвидуальных данных из выбранной директории на диске.
    
    Параметры
    ---------
    year : integer
        Год волны исследования.
    path : string
        Директория волн исследования.
    """
    
    if (year<1994) or (year==1997) or (year==1999):
        print('Волны {0} года не существует.'.format(year))
    else:
        filename=os.listdir(r'{0}\{1}-я волна\ИНДИВИДЫ'.format(path,waves_dict[year][0]))[0]
        return pd.read_spss(r'{0}\{1}-я волна\ИНДИВИДЫ\{2}'.format(path,waves_dict[year][0],filename))
    
#==========================================================================================
 # Загрузка в словарь нескольких волн исследования
def read_period_ind(period,path=os.getcwd()+'\\RLMS_db'):
    """
    Загрузка списка с выбранными волнами данных индивидов из выбранной директории на диске.
    
    Параметры
    ---------
    period : list
        Список волн. 
    path : string
        Директория волн исследования.
    """
    dict_ind_period={}
    for i in period:
        if (i<1994) or (i==1997) or (i==1999):
            print('Волны {0} года не существует.'.format(i))
            continue
        dict_ind_period[i]=read_wave_ind(i,path)
        print('Загружен ',i)
    return dict_ind_period

#==========================================================================================
# Загрузка данных для работы FAST-функций
def FAST_variable_ind(path=os.getcwd()+'\\RLMS_db'):
    """
    Функция, загружающая в среду словарь всех волн исследования индивидов, и сохраняющая его как глобальную переменную.
    
    Параметры
    ---------
    path : string
        Директория волн исследования.
    
    Notes
    -----
    #Написать про FAST-функции
    """
    global FAST_INDS_DFS
    FAST_INDS_DFS=read_period_ind(list(range(1993,2022)),path=path)
     
        

#==========================================================================================
"""
ЧТЕНИЕ ДАННЫХ ДОМОХОЗЯЙСТВ
"""
#==========================================================================================
#==Аналогично для ДХ=======================================================================
#=Загрузка фрейма данных волны выбранного года из папки
def read_wave_hh(year,path=os.getcwd()+'\\RLMS_db'):
    """
    Загрузка выбранной волны (года) данных домашних хозяйств из выбранной директории на диске.
    
    Параметры
    ---------
    year : integer
        Год волны исследования.
    path : string
        Директория волн исследования.
    """
    if (year<1994) or (year==1997) or (year==1999):
        print('Волны {0} года не существует.'.format(year))
    else:
        filename=os.listdir(r'{0}\{1}-я волна\ДОМОХОЗЯЙСТВА'.format(path,waves_dict[year][0]))[0]
        return pd.read_spss(r'{0}\{1}-я волна\ДОМОХОЗЯЙСТВА\{2}'.format(path,waves_dict[year][0],filename))
#==========================================================================================
# Загрузка в словарь нескольких волн исследования
def read_period_hh(period,path=os.getcwd()+'\\RLMS_db'):
    """
    Загрузка списка с выбранными волнами данных домашних хозяйств из выбранной директории на диске.
    
    Параметры
    ---------
    period : list
        Список волн. 
    path : string
        Директория волн исследования.
    """
    dict_hh_period={}
    for i in period:
        if (i<1994) or (i==1997) or (i==1999):
            print('Волны {0} года не существует.'.format(i))
            continue
        dict_hh_period[i]=read_wave_hh(i,path)
        print('Загружен ',i)
    return dict_hh_period
#==========================================================================================
def FAST_variable_hh(path=os.getcwd()+'\\RLMS_db'):
    """
    Функция, загружающая в среду словарь всех волн исследования домашних хозяйств, и сохраняющий как глобальную переменную.
    
    Параметры
    ---------
    path : string
        Директория волн исследования.
    
    Notes
    -----
    #Написать про FAST-функции
    """
    global FAST_HH_DFS
    FAST_HH_DFS=read_period_hh(list(range(1993,2022)),path=path)
#==========================================================================================   
def FAST_variable_ind(path=os.getcwd()+'\\RLMS_db'):
    """
    Функция, загружающая в среду словарь всех волн исследования индивидов, и сохраняющий их как глобальную переменную.
    
    Параметры
    ---------
    path : string
        Директория волн исследования.
    
    Notes
    -----
    #Написать про FAST-функции
    """
    global FAST_IND_DFS
    FAST_IND_DFS=download_period_ind(list(range(1993,2022)),path=path)
#==========================================================================================  
    
#==========================================================================================
def read_rlms(year, var, path=os.getcwd()+'\\RLMS_db',verbose=True):
    """
    Читает уже загруженный датасет. 
    ---------
    year : integer, list, string
        (default 'all')
        Если 'all', то загружает все волны исследования. 
    path : string, optional
        (default )
        Директория базы данных
    var: string
        (default )
        Если 'all' 
        Если 'hh'
        Если 'ind'
    """
    if type(year)==int:
        if var=='hh':
            return read_wave_hh(year=year, path=path, verbose=verbose)
        if var=='ind':
            return read_wave_ind(year=year, path=path, verbose=verbose)
    if type(year)==list:
        if var=='hh':
            return read_period_ind(period=year,path=path, verbose=verbose)
        if var=='ind':
            return read_period_ind(period=year,path=path, verbose=verbose)
    if year=='all':
        if var=='hh':
            return FAST_variable_hh(path=path, verbose=verbose)
        if var=='ind':
            return FAST_variable_ind(path=path, verbose=verbose)
#==========================================================================================   
    
    

# Далее реализовано лишь для индивидов и без FAST-префикса
#==========================================================================================
def good_namer(year, var='ind'):
    # Она должна быть FAST
    # Должно быть уточнение, что это только для индивидов
    """
    Возвращает фрейм данных, с переименованными атрибутами (убран префикс волны).
    
    Параметры
    ---------
    year : integer
        Год волны исследования.
    var: string
        Если значение 'ind' ...
        Если значение 'hh' ...
    
    Notes
    -----
    # Написать про FAST-функции
    """
    if var=='ind':
        save=FAST_INDS_DFS[year].copy(deep=1)
    if var=='hh':
        save=FAST_HH_DFS[year].copy(deep=1)
        
    for i in save.columns:
        bad=waves_dict[year][1].lower()
        if 'id' in i:
            pass
        elif str(bad+'_') in i:
            save=save.rename(columns={i: i[2:]})
        elif bad==i[0]:
            save=save.rename(columns={i: i[1:]})
    return save

#==========================================================================================
def namer_period(period, var='ind'):
    """
    Возвращает словарь фреймов данных, с переименованными атрибутами (убран префикс волны).
    
    Параметры
    ---------
    period : list
        Список волн.
    
    Notes
    -----
    # Написать про FAST-функции
    """
    dict_ind_period={}
    for i in period:
        if (i<1994) or (i==1997) or (i==1999):
            print('Волны {0} года не существует.'.format(i))
            continue
        dict_ind_period[i]=good_namer(i,var)
        print('Исправлен ',i)
    return dict_ind_period
#==========================================================================================













#==========================================================================================
def corrector(year, var='ind'):
    """
    Возвращает словарь фреймов данных, с переименованными атрибутами (убран префикс волны).
    
    Параметры
    ---------
    period : list
        Список волн.
    
    Notes
    -----
    # Написать про FAST-функции
    """
    save=good_namer(year,var)
    if var=='ind':
        save.loc[:,'marst']=save.loc[:,'marst'].cat.rename_categories({'Bдовец (вдова)':'Вдовец (вдова)'})
    if var=='hh':
        pass
    return save
#==========================================================================================
def corrector_period(period,var='ind'):
    """
    Возвращает словарь фреймов данных, с переименованными атрибутами (убран префикс волны).
    
    Параметры
    ---------
    period : list
        Список волн.
    
    Notes
    -----
    # Написать про FAST-функции
    """
    dict_ind_period={}
    for i in period:
        if (i<1994) or (i==1997) or (i==1999):
            print('Волны {0} года не существует.'.format(i))
            continue
        dict_ind_period[i]=corrector(i,var)
        print('Исправлен ',i)
    return dict_ind_period
#==========================================================================================
def FAST_corrector_ind():
    """
    Возвращает словарь фреймов данных, с переименованными атрибутами (убран префикс волны).
    
    Параметры
    ---------
    period : list
        Список волн.
    
    Notes
    -----
    # Написать про FAST-функции
    """
    global FAST_CORRECTED_INDS_DFS
    FAST_CORRECTED_INDS_DFS=corrector_period(waves_dict.keys(),var='ind')
#==========================================================================================
def FAST_corrector_hh():
    """
    Возвращает словарь фреймов данных, с переименованными атрибутами (убран префикс волны).
    
    Параметры
    ---------
    period : list
        Список волн.
    
    Notes
    -----
    # Написать про FAST-функции
    """
    global FAST_CORRECTED_HH_DFS
    FAST_CORRECTED_HH_DFS=corrector_period(waves_dict.keys(),var='hh')














#==========================================================================================
def wave_scanner(codes,reverse=False):
    """
    Возвращает словарь волн исследования, в которых есть данные атрибуты.
    
    Параметры
    ---------
    codes : list, dictionary
        Список стандартизированных (без префиксов) атрибутов.
        Может получить словарь с атрибутами, то в таком случае выдает итоговый словарь в ключами данного словаря атрибутов. 
    reverse : bool, optional
        Если False, то возвращает словарь, где ключи - годы волн, а значения - это найденные атрибуты.
        Если True, то возвращает словарь, где ключи - выбранные коды атрибутов, а значения - годы, в котором есть эти артибуты.
        
    Notes
    -----

    """
    # Не написано reverse
    year_book={}
    nnn=FAST_CORRECTED_INDS_DFS
    if reverse:
        pass
    
    if reverse==False and type(codes)==list:
        for j in list(waves_dict.keys()):
            year_book[j]=list()
            
            for i in nnn[j].columns:
                for s in codes:
                    if s in i.lower():
                        year_book[j].append(s)
    
    if reverse==False and type(codes)==dict:
        reverse_dict={l[i]:k for k, l in codes.items() for i in range(len(l))}
        
    # Написать позже вариант для словаря
    
    new_dict = {a:list(set(b)) for a, b in year_book.items()}
    return new_dict
#==========================================================================================

#==========================================================================================

#==========================================================================================

#==========================================================================================
