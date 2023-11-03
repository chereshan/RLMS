#Задействованные модули
import pandas as pd
import os
import numpy as np
import sys
import requests
import zipfile
import pyreadstat
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
double_reverse_waves_dict={5: [1994, 'A'],
 6: [1995, 'B'],
 7: [1996, 'C'],
 8: [1998, 'D'],
 9: [2000, 'E'],
 10: [2001, 'F'],
 11: [2002, 'G'],
 12: [2003, 'H'],
 13: [2004, 'I'],
 14: [2005, 'J'],
 15: [2006, 'K'],
 16: [2007, 'L'],
 17: [2008, 'M'],
 18: [2009, 'N'],
 19: [2010, 'O'],
 20: [2011, 'P'],
 21: [2012, 'Q'],
 22: [2013, 'R'],
 23: [2014, 'S'],
 24: [2015, 'T'],
 25: [2016, 'U'],
 26: [2017, 'V'],
 27: [2018, 'W'],
 28: [2019, 'X'],
 29: [2020, 'Y'],
 30: [2021, 'Z']}
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
        Если True, то выволит прогресс работы функции. 
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
def read_wave_hh(year,path=os.getcwd()+'\\RLMS_db',verbose=True, renaming=False, what='representative'):
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
    what : ???????
    """
    if what=='full':
        path=path+'_full'
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
            df_fin['year']=reverse_waves_dict[FAST_HH_DFS[year].columns[0][0].upper()][0]
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
        Если 'all', то считывает все волны исследования. Иначе считывает либо год, либо выбранный лист лет. 
    path : string, optional
        (default os.getcwd()+'\\RLMS_db'и)
        Директория базы данных. 
    var: string
        Если 'hh', то читает фрейм данных домашних хозяйств. 
        Если 'ind', то читает фрейм данных индивидов. 
        Если 'all', то читает оба фрейма данных. 
    verbose : bool, optional
        (default True)
        Если True, то отображает прогресс работы функции. 
        Если False, то не отображает прогресс работы функции. 
    renaming : bool, optional
        (default False)
        Если True, то сразу после чтения производит удаление префиксов кодов волн у столбцов, т.е. оссуществляет переименование. 
        Если False, то этого не осуществляется.
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
def columns_renamer(df, year=None, verbose=False):
    """
    Принимает фрейм данных RLMS и возвращает тот же самый фрейм с переименованными колонками, у которых убраны префиксы волн (за исключением идентификационных). Все строки переводит в строчные. Это вызов столбцов по сквозному коду без префикса.
    
    Параметры
    ---------
    df : DataFrame
       Фрейм данных волны исследования
    year : integer, string, optional
        Если в датафрейме нет колонки *redid_h или *redid_i, где первый символ - это кодовая буква волны исследования, то прямое обозначение года волны или буквы волны может потребоваться для работы со срезом данных. 
    verbose : bool, optional
        (default False)
        Если True, то отображает прогресс работы функции. 
        Если False, то не отображает прогресс работы функции.
    """
    df_copy=df.copy(deep=1)
    exceps=['redid_h','id_h','_origsm','_hhwgt',
           'idind','redid_i','id_i','_inwgt',
           'region','psu','status','popul','site','ssu', 'year']
    
    df_copy=df_copy.rename(columns={i:i.lower() for i in list(df_copy.columns)})
    if year==None: 
        if any(df_copy.columns.str.contains(pat='redid_i')):
            year=df_copy.columns[df_copy.columns.str.contains(pat='redid_i')][0][0]
        elif any(df_copy.columns.str.contains(pat='redid_h')):
            year=df_copy.columns[df_copy.columns.str.contains(pat='redid_h')][0][0]
        else:
            print('В фрейме нет столбцов *redid_i или *redid_i для определения префикса волны. Прямо пропишите в аргумент year год волны или ее буквенный код')
            
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
# ['g6', 'g4', 'g1.1', 'g5', 'g1.2', 'g2', 'g3', 'g7']
errors_found={'marst':{'Bдовец (вдова)':'Вдовец (вдова)'},
              'g7':None,
              'g6':{'ЗНАЧИТ. БОЛЕЕ ИСКРЕНЕН И ОТКРЫТ,ЧЕМ БОЛЬШИНСТВО РЕСПОНД.':'ЗНАЧИТЕЛЬНО БОЛЕЕ ИСКРЕНЕН И ОТКРЫТ, ЧЕМ БОЛЬШИНСТВО РЕСПОНДЕНТОВ',
                    'ЗНАЧИТЕЛЬНО БОЛЕЕ ИСКРЕНЕН И ОТКРЫТ, ЧЕМ БОЛЬШИНСТВО РЕС-ТОВ':'ЗНАЧИТЕЛЬНО БОЛЕЕ ИСКРЕНЕН И ОТКРЫТ, ЧЕМ БОЛЬШИНСТВО РЕСПОНДЕНТОВ',
                    '‘ЗНАЧИТЕЛЬНО БОЛЕЕ ИСКРЕНЕН И ОТКРЫТ, ЧЕМ БОЛЬШИНСТВО РЕСПОНД’':'ЗНАЧИТЕЛЬНО БОЛЕЕ ИСКРЕНЕН И ОТКРЫТ, ЧЕМ БОЛЬШИНСТВО РЕСПОНДЕНТОВ',
                    'ЗНАЧИТЕЛЬНО БОЛЕЕ ИСКРЕНЕН И ОТКРЫТ, ЧЕМ БОЛЬШ. РЕСПОНДЕНТОВ':'ЗНАЧИТЕЛЬНО БОЛЕЕ ИСКРЕНЕН И ОТКРЫТ, ЧЕМ БОЛЬШИНСТВО РЕСПОНДЕНТОВ',
                    'ЗНАЧИТЕЛЬНО БОЛЕЕ ИСКРЕНЕН И ОТКРЫТ, ЧЕМ БОЛЬШИНСТВО РЕСПОНД':'ЗНАЧИТЕЛЬНО БОЛЕЕ ИСКРЕНЕН И ОТКРЫТ, ЧЕМ БОЛЬШИНСТВО РЕСПОНДЕНТОВ',
                    
                    'ИСКРЕНЕН И ОТКРЫТ ТАК ЖЕ, КАК БОЛЬШИНСТВО РЕСПОНДЕНТОВ':'ИСКРЕНЕН И ОТКРЫТ ТАК ЖЕ, КАК БОЛЬШИНСТВО РЕСПОНДЕНТОВ',
                    '‘ИСКРЕНЕН И ОТКРЫТ ТАК ЖЕ, КАК БОЛЬШИНСТВО РЕСПОНДЕНТОВ’':'ИСКРЕНЕН И ОТКРЫТ ТАК ЖЕ, КАК БОЛЬШИНСТВО РЕСПОНДЕНТОВ',
                    'НЕТ ОТВЕТА':'НЕТ ОТВЕТА',
                    '‘НЕТ ОТВЕТА’':'НЕТ ОТВЕТА',
                    'ОЧЕНЬ ЗАКРЫТЫЙ, НЕИСКРЕННИЙ':'ОЧЕНЬ ЗАКРЫТЫЙ, НЕИСКРЕННИЙ',
                   '‘ОЧЕНЬ ЗАКРЫТЫЙ, НЕИСКРЕННИЙ’':'ОЧЕНЬ ЗАКРЫТЫЙ, НЕИСКРЕННИЙ'},
              'g4':{'‘ИНОГДА НЕРВНИЧАЛ’':'ИНОГДА НЕРВНИЧАЛ',
                    '‘НЕРВНИЧАЛ’':'НЕРВНИЧАЛ',
                    '‘НЕТ ОТВЕТА’':'НЕТ ОТВЕТА',
                    '‘ЧУВСТВОВАЛ СЕБЯ СВОБОДНО’':'ЧУВСТВОВАЛ СЕБЯ СВОБОДНО'
              },
              'g1.1':None,
              'g1.2':None,
              'g5':{'ЗНАЧИТЕЛЬНО СООБРАЗИТЕЛЬНЕЕ, ЧЕМ БОЛЬШИНСВО РЕСПОНДЕНТОВ':'ЗНАЧИТЕЛЬНО СООБРАЗИТЕЛЬНЕЕ, ЧЕМ БОЛЬШИНСТВО РЕСПОНДЕНТОВ',
                    'ЗНАЧИТЕЛЬНО СООБРАЗИТЕЛЬНЕЕ, ЧЕМ БОЛЬШИНСТВО РЕСПОНДЕНТОВ':'ЗНАЧИТЕЛЬНО СООБРАЗИТЕЛЬНЕЕ, ЧЕМ БОЛЬШИНСТВО РЕСПОНДЕНТОВ',
                    'ЗНАЧИТЕЛЬНО СООБРАЗИТЕЛЬНЕЕ, ЧЕМ БОЛЬШИНСТВО РЕСПОНДЕНТОВ':'ЗНАЧИТЕЛЬНО СООБРАЗИТЕЛЬНЕЕ, ЧЕМ БОЛЬШИНСТВО РЕСПОНДЕНТОВ',
                    'НЕСООБРАЗИТЕЛЬНЫЙ, НУЖДАЛСЯ В ПОВТОРНОМ ЧТЕНИИ ВОПРОСОВ':'НЕСООБРАЗИТЕЛЬНЫЙ, НУЖДАЛСЯ В ПОВТОРНОМ ЧТЕНИИ ВОПРОСОВ',
                    'НЕСООБРАЗИТЕЛЬНЫЙ, НУЖДАЛСЯ В ПОВТОРНОМ ЧТЕНИИ ВОПРОСОВ':'НЕСООБРАЗИТЕЛЬНЫЙ, НУЖДАЛСЯ В ПОВТОРНОМ ЧТЕНИИ ВОПРОСОВ',
                    'НЕСООБРАЗИТЕЛЬНЫЙ, НУЖДАЛСЯ В ДОПОЛНИТЕЛЬНЫХ ОБЪЯСНЕНИЯХ':'НЕСООБРАЗИТЕЛЬНЫЙ, НУЖДАЛСЯ В ПОВТОРНОМ ЧТЕНИИ ВОПРОСОВ',
                    'ОЧЕНЬ НЕСООБРАЗИТЕЛЬНЫЙ':'ОЧЕНЬ НЕСООБРАЗИТЕЛЬНЫЙ',
                    'ОЧЕНЬ НЕСООБРАЗИТЕЛЬНЫЙ':'ОЧЕНЬ НЕСООБРАЗИТЕЛЬНЫЙ',
                    'ОЧЕНЬ НЕСООБРАЗИТЕЛЬНЫЙ':'ОЧЕНЬ НЕСООБРАЗИТЕЛЬНЫЙ',
                    'СООБРАЗИТЕЛЕН, КАК БОЛЬШИНСТВО РЕСПОНДЕНТОВ':'СООБРАЗИТЕЛЕН, КАК БОЛЬШИНСТВО РЕСПОНДЕНТОВ',
                    'СООБРАЗИТЕЛЕН, КАК БОЛЬШИНСТВО РЕСПОНДЕНТОВ':'СООБРАЗИТЕЛЕН, КАК БОЛЬШИНСТВО РЕСПОНДЕНТОВ',
                    'СООБРАЗИТЕЛЕН, КАК БОЛЬШИНСТВО РЕСПОНДЕНТОВ':'СООБРАЗИТЕЛЕН, КАК БОЛЬШИНСТВО РЕСПОНДЕНТОВ'
              },
              'g2':None,
              'g3':None
             
             }

#==========================================================================================
"""
ЧТЕНИЕ СТОРОННИХ ДАННЫХ: Blanciforti86, USMeatConsump
"""        
#==========================================================================================
# import RLMS_HSE.reading as readrlms
# from importlib import reload  # Python 3.4+
# readrlms = reload(readrlms)
#==========================================================================================
#==========================================================================================
"""
КОРРЕКТИРОВКА НЕКОРРЕКТНЫХ ОТВЕТОВ
"""        
#==========================================================================================

#==========================================================================================
# Далее идут корректоры, которые нужно было бы интегрировать в чтение
# Надо, чтобы все категории были в .upper(), не имели первых пробелов, были одинаково написаны
#==========================================================================================
def cat_corrector(df,by= ['g6', 'g4', 'g1.1', 'g5', 'g1.2', 'g2', 'g3','g7']):
    """

    
    Параметры
    ---------
    df : DataFrame
        Фрейм данных, ответы в котором будут откорректированы. 
        
    """
    df1=df.copy(deep=1)
    for feature in by:
        if feature in df1.columns:
            if errors_found.get(feature, 'not in dict')==None:
                df1.loc[:,feature]=df1.loc[:,feature].cat.rename_categories(lambda x: x.lstrip().upper().replace('‘','').replace('’',''))
            elif errors_found.get(feature, 'not in dict')=='not in dict':
                pass
            else:
                df1.loc[:,feature]=df1.loc[:,feature].cat.rename_categories(lambda x: x.lstrip().upper().replace('‘','').replace('’',''))
                df1.loc[:,feature]=df1.loc[:,feature].cat.rename_categories(errors_found[feature])
    return df1
#==========================================================================================
def check_cats(year1, df, cats=['g6', 'g4', 'g1.1', 'g5', 'g1.2', 'g2', 'g3', 'g7']):
    """
    Проверяет пользовательский словарь волн исследования (с переименованными столюцами) на факт того, что категории выбранных фичей данного года совпадают с фичами других лет в словаре датафреймов. Даная функция необходима для корректировки несовпадающих ответов. Используется для пополнения словаря исправлений.  
    """
    dict_of_cats={}
    for year_3 in df.keys():
        df_copy=df[year_3].copy(deep=1)
        dict_of_cats[year_3]={}
        for i in cats:
            if i in df_copy.columns:
                dict_of_cats[year_3][i]=df_copy.loc[:,i].cat.categories
            
    res={}
    for feature in dict_of_cats[year1].keys():
        res[feature]={}
        for year2 in dict_of_cats.keys():

            if feature in list(dict_of_cats[year2].keys()):
                res[feature][year2]=(set(dict_of_cats[year1][feature])==set(dict_of_cats[year2][feature]))
            else:
                res[feature][year2]=np.nan
    return pd.DataFrame(res)
#==========================================================================================
def drop_no_ans(df, by=['g6', 'g4', 'g1.1', 'g5', 'g7', 'g1.2', 'g2', 'f14', 'g3']):
    """
    Функция дропает из датафрейма ответы вида 'нет ответа' по выбранным столбцам. 
    """
    df_f=df.copy()
    nones=['ЗАТРУДНЯЮСЬ ОТВЕТИТЬ', 'НЕТ ОТВЕТА', 'ОТКАЗ ОТ ОТВЕТА',
          '‘ЗАТРУДНЯЮСЬ ОТВЕТИТЬ’', '‘НЕТ ОТВЕТА’', '‘ОТКАЗ ОТ ОТВЕТА’',
          ' ЗАТРУДНЯЮСЬ ОТВЕТИТЬ', ' НЕТ ОТВЕТА', ' ОТКАЗ ОТ ОТВЕТА',
          'ОТКАЗ', 999998.0]
    if by==None:
        for column in df.columns:
            for none in nones:
                df_f=df_f.loc[df_f.loc[:,column]!=none]
            if df_f.loc[:,column].dtype.name=='category':
                df_f.loc[:,column]=df_f.loc[:,column].cat.remove_unused_categories()
    else:
        for column in by:
            if column in df_f.columns:
                for none in nones:
                    df_f=df_f.loc[df_f.loc[:,column]!=none] 
                if df_f.loc[:,column].dtype.name=='category':
                    df_f.loc[:,column]=df_f.loc[:,column].cat.remove_unused_categories()
    return df_f
#==========================================================================================
def isfloat(num):
    """
    Функция, которая проверяет конвертируема ли строка в float.
    """
    try:
        float(num)
        return True
    except ValueError:
        return False
#==========================================================================================
def convert_to_float(df, by=['g6', 'g4', 'g1.1', 'g5', 'g7', 'g1.2', 'g2', 'f14', 'g3']):
    """
    После успешного дропа всех "нет ответа" тектсовые фичи отсаются текстовыми, а численные имеют только численные значения. Это можно использовать для конвертации всех фич, которые могут быть конвертированы в численные. 
    """
    df_f=df.copy()
    if by==None:
        for column in df.columns:
            if df_f.loc[:,column].apply(isfloat).all():
                df_f.loc[:,column]=df_f.loc[:,column].astype(str).astype(float)
                
    else:
        for column in by:
            if column in df_f.columns:
                if df_f.loc[:,column].apply(isfloat).all():
                    df_f.loc[:,column]=df_f.loc[:,column].astype(str).astype(float)
    return df_f

#==========================================================================================
def full_preprocessing(df, features):
    """
    Должно
    """
    df_f=df.loc[:, features]
    df_f=cat_corrector(df_f, by=features)
    df_f=drop_no_ans(df_f, by=features)
    df_f=convert_to_float(df_f, by=features)
    return df_f
    
#==========================================================================================
"""
ПАНЕЛЬНЫЕ ДАННЫЕ
"""
#==========================================================================================
def panel_dict(df_dict, year1, year2):
    """
    
    """
    id_col=waves_dict[year1][1].lower()+'id_h'
    sets=[set(df_dict[year][id_col].dropna().astype(int).values) for year in range(year1,year2+1)]
    panel_indexes=list(set.intersection(*sets))
    panel_dict={}
    for year in range(year1,year2+1):
        panel_dict[year]=df_dict[year][df_dict[year][id_col].isin(panel_indexes)]
    return panel_dict
#==========================================================================================




#==========================================================================================
"""
Сквозной поисковик вопросов
"""
#==========================================================================================
def wave_scanner(codes,reverse=False, where='ind'):
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
    ОЧЕНЬ СЫРАЯ!
    """
    # Не написано reverse
    year_book={}
    if where=='ind':
        nnn=FAST_IND_DFS
    elif where=='hh':
        nnn=FAST_HH_DFS
    
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
        return reverse_dict
        
    # Написать позже вариант для словаря
    
    new_dict = {a:list(set(b)) for a, b in year_book.items()}
    return new_dict