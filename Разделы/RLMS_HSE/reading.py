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
reverse_waves_dict={'A': [1994, 5],
                    'B': [1995, 6],
                    'C': [1996, 7],
                    'D': [1998, 8],
                    'E': [2000, 9],
                    'F': [2001, 10],
                    'G': [2002, 11],
                    'H': [2003, 12],
                    'I': [2004, 13],
                    'J': [2005, 14],
                    'K': [2006, 15],
                    'L': [2007, 16],
                    'M': [2008, 17],
                    'N': [2009, 18],
                    'O': [2010, 19],
                    'P': [2011, 20],
                    'Q': [2012, 21],
                    'R': [2013, 22],
                    'S': [2014, 23],
                    'T': [2015, 24],
                    'U': [2016, 25],
                    'V': [2017, 26],
                    'W': [2018, 27],
                    'X': [2019, 28],
                    'Y': [2020, 29],
                    'Z': [2021, 30]}
#==========================================================================================
def download_rlms_db(year='all', path=os.getcwd(), var='all', del_zip=True, verbose=True):
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
def read_wave_ind(year,path=os.getcwd()+'\\RLMS_db', verbose=True, renaming=False):
    """
    Загрузка выбранной волны инидвидуальных данных из выбранной директории на диске.
    
    Параметры
    ---------
    year : integer
        Год волны исследования.
    path : string, optional
        (default os.getcwd()+'\\RLMS_db')
        Директория волн исследования.
    verbose : bool, optional
        (default True)
        Если True, то отображает прогресс работы функции. 
        Если False, то не отображает прогресс работы функции. 
   renaming : bool, optional
        (default False)
        Если True, то сразу после чтения производит удаление префиксов кодов волн у столбцов. 
        Если False, то этого не делает
    """
    
    if (year<1994) or (year==1997) or (year==1999):
        if verbose==True:
            print('Волны {0} года не существует.'.format(year))
            return None
    else:
        filename=os.listdir(r'{0}\{1}-я волна\ИНДИВИДЫ'.format(path,waves_dict[year][0]))[0]
        
        df_fin=pd.read_spss(r'{0}\{1}-я волна\ИНДИВИДЫ\{2}'.format(path,waves_dict[year][0],filename))
        if verbose==True:
            print('Загружен {0}'.format(year))
        if renaming==True:
            df_fin=columns_renamer(df_fin,year=year,verbose=verbose)
        return df_fin
    
#==========================================================================================
 # Загрузка в словарь нескольких волн исследования
def read_period_ind(period,path=os.getcwd()+'\\RLMS_db', verbose=True, renaming=False):
    """
    Загрузка списка с выбранными волнами данных индивидов из выбранной директории на диске.
    
    Параметры
    ---------
    period : list
        Список волн. 
    path : string
        Директория волн исследования.
    verbose : bool, optional
        (default True)
        Если True, то отображает прогресс работы функции. 
        Если False, то не отображает прогресс работы функции. 
    renaming : bool, optional
        (default False)
        Если True, то сразу после чтения производит удаление префиксов кодов волн у столбцов. 
        Если False, то этого не делает
    """
    dict_ind_period={}
    for i in period:
        iteration=read_wave_ind(i,path,verbose=verbose, renaming=renaming)
        if iteration is None:
            continue
        else:
            dict_ind_period[i]=iteration
    return dict_ind_period

#==========================================================================================
# Загрузка данных для работы FAST-функций
def FAST_variable_ind(path=os.getcwd()+'\\RLMS_db', verbose=True, renaming=False):
    """
    Функция, загружающая в среду словарь всех волн исследования индивидов, и сохраняющая его как глобальную переменную.
    
    Параметры
    ---------
    path : string
        Директория волн исследования.
    verbose : bool, optional
        (default True)
        Если True, то отображает прогресс работы функции. 
        Если False, то не отображает прогресс работы функции. 
    renaming : bool, optional
        (default False)
        Если True, то сразу после чтения производит удаление префиксов кодов волн у столбцов. 
        Если False, то этого не делает
    """
    global FAST_IND_DFS
    FAST_IND_DFS=read_period_ind(list(range(1993,2022)),path=path,verbose=verbose, renaming=renaming)
#==========================================================================================
"""
ЧТЕНИЕ ДАННЫХ ДОМОХОЗЯЙСТВ
"""
#==========================================================================================    
#==========================================================================================
def read_wave_hh(year,path=os.getcwd()+'\\RLMS_db',verbose=True, renaming=False):
    """
    Загрузка выбранной волны (года) данных домашних хозяйств из выбранной директории на диске.
    
    Параметры
    ---------
    year : integer
        Год волны исследования.
    path : string
        Директория волн исследования.
    verbose : bool, optional
        (default True)
        Если True, то отображает прогресс работы функции. 
        Если False, то не отображает прогресс работы функции. 
    renaming : bool, optional
        (default False)
        Если True, то сразу после чтения производит удаление префиксов кодов волн у столбцов. 
        Если False, то этого не делает
    """
    if (year<1994) or (year==1997) or (year==1999):
        if verbose==True:
            print('Волны {0} года не существует.'.format(year))
    else:
        filename=os.listdir(r'{0}\{1}-я волна\ДОМОХОЗЯЙСТВА'.format(path,waves_dict[year][0]))[0]
        df_fin=pd.read_spss(r'{0}\{1}-я волна\ДОМОХОЗЯЙСТВА\{2}'.format(path,waves_dict[year][0],filename))
        
        if verbose==True:
            print('Загружен {0}'.format(year))
        if renaming==True:
            df_fin=columns_renamer(df_fin,year=year,verbose=verbose)    
        return df_fin        
    
    
#==========================================================================================
# Загрузка в словарь нескольких волн исследования
def read_period_hh(period,path=os.getcwd()+'\\RLMS_db',verbose=True, renaming=False):
    """
    Загрузка списка с выбранными волнами данных домашних хозяйств из выбранной директории на диске.
    
    Параметры
    ---------
    period : list
        Список волн. 
    path : string
        Директория волн исследования.
    verbose : bool, optional
        (default True)
        Если True, то отображает прогресс работы функции. 
        Если False, то не отображает прогресс работы функции. 
    renaming : bool, optional
        (default False)
        Если True, то сразу после чтения производит удаление префиксов кодов волн у столбцов. 
        Если False, то этого не делает
    """
    dict_hh_period={}
    for i in period:
        iteration=read_wave_hh(i,path,verbose=verbose,renaming=renaming)
        if iteration is None:
            continue
        else:
            dict_hh_period[i]=iteration
    return dict_hh_period    
#==========================================================================================
def FAST_variable_hh(path=os.getcwd()+'\\RLMS_db',verbose=True,renaming=False):
    """
    Функция, загружающая в среду словарь всех волн исследования домашних хозяйств, и сохраняющий как глобальную переменную.
    
    Параметры
    ---------
    path : string
        Директория волн исследования.
    verbose : bool, optional
        (default True)
        Если True, то отображает прогресс работы функции. 
        Если False, то не отображает прогресс работы функции. 
    renaming : bool, optional
        (default False)
        Если True, то сразу после чтения производит удаление префиксов кодов волн у столбцов. 
        Если False, то этого не делает
    """
    global FAST_HH_DFS
    FAST_HH_DFS=read_period_hh(list(range(1993,2022)),path=path,verbose=verbose,renaming=renaming)
#==========================================================================================    
"""
УНИВЕРСАЛЬНЫЙ РИДЕР
"""
#==========================================================================================
def read_rlms(year, var, path=os.getcwd()+'\\RLMS_db',verbose=True, renaming=False):
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
    verbose : bool, optional
        (default True)
        Если True, то отображает прогресс работы функции. 
        Если False, то не отображает прогресс работы функции. 
    renaming : bool, optional
        (default False)
        Если True, то сразу после чтения производит удаление префиксов кодов волн у столбцов. 
        Если False, то этого не делает
    """
    if type(year)==int:
        if var=='hh':
            return read_wave_hh(year=year, path=path,verbose=verbose, renaming=renaming)
        if var=='ind':
            return read_wave_ind(year=year, path=path,verbose=verbose, renaming=renaming)
    if type(year)==list:
        if var=='hh':
            return read_period_ind(period=year,path=path,verbose=verbose, renaming=renaming)
        if var=='ind':
            return read_period_ind(period=year,path=path,verbose=verbose, renaming=renaming)
    if year=='all':
        if var=='hh':
            return FAST_variable_hh(path=path,verbose=verbose, renaming=renaming)
        if var=='ind':
            return FAST_variable_ind(path=path,verbose=verbose, renaming=renaming)
#==========================================================================================    
"""
ПОДГОТОВКА ДАТАСЕТА К РАБОТЕ: УДАЛЕНИЕ ПРЕФИКСОВ И ПЕРЕВОД В СТРОЧНЫЕ
"""
#==========================================================================================        
# Далее реализовано лишь для индивидов и без FAST-префикса
#==========================================================================================
def columns_renamer(df,year=None, verbose=True):
    """
    Принимает фрейм данных RLMS и возвращает тот же самый фрейм с переименованными колонками, у которых убраны префиксы волн (за исключением идентификационных). Все строки переводит в строчные. Это вызов столбцов по сквозному коду без префикса.
    
    Параметры
    ---------
    df : DataFrame
       Фрейм данных волны исследования
    year: integer, string, optional
        Если в датафрейме нет колонки *redid_h или *redid_i, где первый символ - это кодовая буква волны исследования, то прямое обозначение года волны или буквы волны может потребоваться для работы со срезом данных. 
    verbose : bool, optional
        (default True)
        Если True, то отображает прогресс работы функции. 
        Если False, то не отображает прогресс работы функции.
    """
    df_copy=df.copy(deep=1)
    exceps=['redid_h','id_h','_origsm','_hhwgt',
           'idind','redid_i','id_i','_inwgt',
           'region','psu','status','popul','site','ssu']
    
    df_copy=df_copy.rename(columns={i:i.lower() for i in list(df_copy.columns)})
    if year==None: 
        if any(df_copy.columns.str.contains(pat='redid_i')):
            year=df_copy.columns[df_copy.columns.str.contains(pat='redid_i')][0][0]
        elif any(df_copy.columns.str.contains(pat='redid_h')):
            year=df_copy.columns[df_copy.columns.str.contains(pat='redid_h')][0][0]
        else:
            print('В фрейме нет стобцов *redid_i или *redid_i для определения префикса волны. Прямо пропишите в аргумент year год волны или буквенный код волны')
            
    if year!=None:
        if type(year)==int:
            year=waves_dict[year][1].lower()
                                                      
    for i in df_copy.columns:
        #Пропускаем идентификационные колонки
        if any([j in i.lower() for j in exceps]):
                 pass
        else:
            if str(year+'_') in i:       
                df_copy=df_copy.rename(columns={i: i[2:]})
            elif year==i[0]:
                df_copy=df_copy.rename(columns={i: i[1:]})
    if verbose==True:
        print(r'Переименован {0}'.format(reverse_waves_dict[year.upper()][0]))
    return df_copy
#==========================================================================================
"""
ИСПРАВЛЕНИЕ (ЗАВЕДОМО ИЛИ НЕТ) ОШИБОЧНЫХ ОТВЕТОВ
"""        
#==========================================================================================
# Обнаруженные ошибочные ответы
errors_found={'Bдовец (вдова)':'Вдовец (вдова)'
             }











# Далее идут корректоры, которые нужно было бы интегрировать в чтение







#==========================================================================================
def corrector(year, var='ind'):
    """
  ?????????????????????????????????????????????????????//
    
    Параметры
    ---------
    period : list
        Список волн.
    var : string
        
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
    ????????????????????????????????????????
    
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
    ??????????????????????????????????????????????????/
    
    Параметры
    ---------
    period : list
        Список волн.
    
    Notes
    -----
    # Написать про FAST-функции
    """
    global FAST_CORRECTED_IND_DFS
    FAST_CORRECTED_IND_DFS=corrector_period(waves_dict.keys(),var='ind')
#==========================================================================================
def FAST_corrector_hh():
    """
    ???????????????????????????????????????????????????//
    
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
"""
Сквозной поисковик вопросов
"""
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
    nnn=FAST_IND_DFS
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
