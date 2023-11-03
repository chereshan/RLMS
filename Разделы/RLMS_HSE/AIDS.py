"""
ЗДЕСЬ ВСТАВИТЬ ДОКСТРИНГ ПОДМОДУЛЯ
Данный модуль содержит все функции, необходимые для имплементации AIDS-модели на данных домашних хозяйств RLMS. 
# Надо переименовать функции

Что надо сделать в рамках рефактора: 
1. [ ] Прописать все докстринги
2. [ ] Сортировать пееременные 
3. [ ] PEP8
4. [ ] 


"""

import pandas as pd
import os
from RLMS_HSE.reading import read_wave_hh,  reverse_waves_dict, full_preprocessing
from numpy import inf, nan, log, float64
import matplotlib.pyplot as plt 
from statsmodels.api import add_constant, OLS
from statsmodels.formula.api import ols
from linearmodels.system import SUR
from scipy import stats
from collections import OrderedDict


food_codebook={
 'e1.1': 'белый хлеб, кг.',
 'e1.2': 'черный хлеб, кг.',
 'e1.3': 'рис, другая крупа, кг.',
 'e1.4': 'мука, кг.',
 'e1.5': 'макаронные изделия, кг.',
 'e1.6': 'картофель, кг.',
 'e1.7': 'овощные консервы, без солений, кг.',
 'e1.8': 'капуста, включая квашеную, кг.',
 'e1.9': 'огурцы, включая соленые, кг.',
 'e1.10': 'помидоры, включая соленые, кг.',
 'e1.11': 'свеклу, морковь и другие корнеплоды, кг.',
 'e1.12': 'лук, чеснок, кг.',
 'e1.13': 'кабачки, тыквы и тому подобное, кг.',
 'e1.14': 'другие овощи, кг.',
 'e1.15': 'арбузы, дыни, включая соленые и сушеные, кг.',
 'e1.16': 'фруктово-ягодные консервы, кг.',
 'e1.17': 'свежие ягоды, кг.',
 'e1.18': 'свежие фрукты, кг.',
 'e1.19': 'сушеные фрукты и ягоды, кг.',
 'e1.20': 'орехи, семечки, кг.',
 'e1.21': 'мясные консервы, кг.',
 'e1.22': 'говядина, телятина, кг.',
 'e1.23': 'баранина, козлятина, кг.',
 'e1.24': 'свинина, кг.',
 'e1.25': 'субпродукты: печень, почки, кг.',
 'e1.26': 'птица, кг.',
 'e1.27': 'сало, другие животные жиры, кг.',
 'e1.28': 'колбасные изделия, копчености, кг.',
 'e1.29': 'мясных полуфабрикатов, кг.',
 'e1.30': 'молочные консервы, сухое молоко, кг.',
 'e1.31': 'молока, кроме сухого, л.',
 'e1.32': 'кисломолочные продукты: кефир, йогурт и другие, л.',
 'e1.33': 'сметана, сливки, л.',
 'e1.34': 'масло животное, кг.',
 'e1.35': 'творог, сырковая масса, кг.',
 'e1.36': 'сыр, брынза, кг.',
 'e1.37': 'мороженое, кг.',
 'e1.38': 'масло растительное, л.',
 'e1.39': 'маргарин, кг.',
 'e1.40': 'сахар, кг.',
 'e1.41': 'конфеты, шоколад, кг.',
 'e1.42': 'варенье, джем, кг.',
 'e1.43': 'меда, л.',
 'e1.44': 'печенье, пирожные, торты, вафли, пряники, сдобные булочки, кг.',
 'e1.45': 'яица, шт.',
 'e1.46': 'рыба свежая, мороженая, соленая, сушеная, рыбные полуфабрикаты, кг.',
 'e1.47': 'рыбные консервы, кг.',
 'e1.58': 'морепродукты, кг.',
 'e1.59': 'полуфабрикаты, не считая мясных и рыбных, кг.',
 'e1.48': 'чай, кг.',
 'e1.49': 'кофе, кофейные напитки, какао, кг.',
 'e1.50': 'безалкогольные напитки, соки, л.',
 'e1.51': 'соль, другие специи, различные соусы, кг. ',
 'e1.52': 'грибы, кг.',
 'e1.53': 'водка, л.',
 'e1.54': 'вино, другие ликероводочные изделия, л.',
 'e1.55': 'пиво, л.',
 'e1.56': 'табачные изделия, пачек'}

# Скорее всего стоит добавить е1.57 жевательные резинки и их несущественность как-то обрабатывать
#==========================================================================================
# Далее идут рутины чистки данных. Скорее всего нужно обозначение их не пользовательских функций

#==========================================================================================
"""
ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ, НЕОБХОДИМЫЕ ДЛЯ ПЕРЕИМЕНОВАНИЯ СТОЛБЦОВ В food_df()
"""
#==========================================================================================
def name_cutter(df,what):
    """
    Вспомогательная функция, которая обрезает имена колонок датафрейма следующим образом:
    Дано (1)е1.57(2).
    Если what='wave', то обрезает (1).
    Если what='quest', то обрезает (2).
    """
    if what=='wave' or what=='waves':
        for i in range(len(df.columns)):
            df.rename(columns={ df.columns[i]: df.columns[i][1:]}, inplace = True)
        return df
    if what=='quest' or what=='quests':
        for i in range(len(df.columns)):
            df.rename(columns={ df.columns[i]: df.columns[i][:-1]}, inplace = True)
        return df
#==============================================    
def food_renamer(df):
    """
    Вспомогательная функция, которая переводит названия колонок в формате кодов в формат названий продуктов. 
    """
    df=name_cutter(df=df,what='wave')
    df=name_cutter(df=df,what='quest')
    for i in df.columns: 
        df=df.rename(columns={i: food_codebook[i]})
    return df
#==========================================================================================
"""
ФУНКЦИЯ, УДАЛЯЮЩАЯ НАБЛЮДЕНИЯ С НЕКОРРЕКТНЫМИ ОТВЕТАМИ (НЕОБХОДИМА ДЛЯ ПОСТРОЕНИЯ ФРЕЙМА ИНДИВИДУАЛЬНЫХ ЦЕН)
"""
#==========================================================================================
no_answer=['ЗАТРУДНЯЮСЬ ОТВЕТИТЬ','НЕТ ОТВЕТА',
           ' НЕТ ОТВЕТА',
           ' ЗАТРУДНЯЮСЬ ОТВЕТИТЬ',
           'НЕТ ОТВЕТА+E7898',
           'ОТКАЗ ОТ ОТВЕТА',
           ' ОТКАЗ ОТ ОТВЕТА',
           'нет ответа', 'ОТКАЗ']
#==========================================================================================
def no_ans_dropper(df1,df2):
    """
    Вспомогательная функция, которая ищет в двух входных датафреймаха df1 и df2 все наблюдения с ответами типа "НЕТ ОТВЕТА" и удаляет их их первого датафрейма. Также заменяет пропуски (nan) на нули. 
    """
#     Ищем в датафрейме все наблюдения, в которых есть ответы категории "Нет ответа"
    bad_households1=[]
    for i in range(len(df1.columns)):
        now_ans_index=df1.loc[df1.iloc[:,i].isin(no_answer)].index
        bad_households1=[*bad_households1,*list(now_ans_index)]
#     Аналогично для второго  
    bad_households2=[]
    for i in range(len(df2.columns)):
        now_ans_index=df2.loc[df2.iloc[:,i].isin(no_answer)].index
        bad_households2=[*bad_households2,*list(now_ans_index)] 
    
#     Объединяем некорректные наблюдения двух датафреймов и дропаем их из первого
    all_bad_households=list(set(bad_households1)|set(bad_households2))
    df1=df1.drop(all_bad_households).copy()
    
#     Заменяем в первом датафрейме все пропуски на нули
    df1=df1.astype(str).replace('nan', '0').astype(float).copy()
    return df1

#==========================================================================================
"""
ГЕНЕРАТОР ДАТАФРЕЙМА ПРОДУКТОВ ПИТАНИЯ
"""
#==========================================================================================
# Функция, которая принимает на вход датафрейм (имеющий или не имеющий стодбец-идентификатор волны)
# и выдающая: количества, индивидуальные цены, суммарные затраты
# вывод количеств и затрат может быть противоречивым с точки зрения ощибочных цен (cost/quant, где quant=0)
# но цены всегда должны быть непротиворечивыми
# В функции должен быть флаг-аргумент, который делает вывод непротиворечивым
# Не повторять функционал других функций, например, функции переименования 
def food_df(df, var, verbose=False, path=os.getcwd()+'\\RLMS_db', contradict = False, cut=['wave', 'quest'], add_features = None, drops=['e1.57','e1.58','e1.59'], correct48=True):
    """
    Генерирует фреймы данных потребления ДХ продуктов питания трех видов (по выбору): расходы ДХ на продукты по видам, количества купленных ДХ продуктов по видам, индивидуальные цены продуктов для каждого ДХ. 
    Автоматически производит чистку данных, если в аргументах не указано обратное. 
    
    Параметры
    ---------
    df : DataFrame, integer, string
        Фрейм данных, год исследования, кодовый символ исследования.
    var : string
        Если 'cost' или 'costs', то конечным датафреймом будет фрейм расходов на продукты питания. 
        Если 'quant' или 'quants', то конечным датафреймом будет фрейм количеств купленных продуктов питания.
        Если 'price' или 'prices', то конечным датафреймом будет фрейм индивидуальных цен продуктов питания.
    verbose : bool, optional
        (default False)
        Если True, то выволит прогресс работы функции.
    path : string, optional
        (default os.getcwd()+'\\RLMS_db')
        Путь на диске к базе данных RLMS.
    contradict : bool, optional
        (default False)
        Параметр, который необходимо должен быть False, если искомая переменная var='price' или 'prices'.
        Если True, то выводит фрейм данных с неотфильтрованными от "НЕТ ОТВЕТА" наблюдениями и незамененными на нули nan-ответами. 
        Если False, то фильтрует итоговый фрейм данных от наблюдений с ответами категории "НЕТ ОТВЕТА" и заменяет nan-ответы нулями. 
    cut : list, string, optional
        (default ['wave', 'quest'])
        Формат названий колонок конечного фрейма данных. 
        Если содержит 'wave' или 'waves', то отрезает от имен колонок код-символ волны.
        Если содержит 'quest' или 'quests', то отрезает от имен колонок код-символ подвопроса. 
        Если равняется 'name' или 'names', то переименует имена колонок в явный формат. 
        Если равняется None, то сохраняет исходные имена колонок.     
    drops : list, optional
        (default ['e1.57','e1.58','e1.59'])
        Не все продукты встречаются в каждом году исследования и потому может иметь смысл исключить из выборки те, что не встречаются в фрейме данных каждой волны RLMS.
    add_features : list, optional
        Список кодов (без префикса волны) дополнительных столбцов, которые должны быть в итоговом датафрейме.
        correct
    """
    # Тут все, что связано с типом df
    prefix=''
    if type(df)==str:
        df=reverse_waves_dict[df.upper()][0]
    if type(df)==int:
        df=read_wave_hh(year=df, path=path, verbose=verbose, renaming=False)
        df=df.rename(columns={i:i.lower() for i in list(df.columns)})
        prefix=df.columns[df.columns.str.contains(pat='e1.1c')][0][0]
    
    else:
        df=df.rename(columns={i:i.lower() for i in list(df.columns)})
        if any(df.columns.str.contains(pat='e1.1c')):
            id_column=df.columns[df.columns.str.contains(pat='e1.1c')][0]
            if id_column=='e1.1c':
                df=df
                prefix=''
                if 'wave' in cut:
                    if cut=='wave':
                        cut=None
                    else:
                        cut.remove('wave')
                if 'waves' in cut:
                    if cut=='waves':
                        cut=None
                    else:
                        cut.remove('waves')
            else:
                prefix=id_column[0]
    # Здесь кончается часть, где определяется тип входных данных
    
    if add_features is not None:
        df_copy=df.copy(deep=True)
        

    if contradict==True:
        if var in ['cost','costs']:
            food_cost=df.loc[:,[prefix+i+'c' for i in [j for j in food_codebook.keys() if j not in drops]]]
            df=food_cost
        
        if var in ['quant','quants']:        
            food_quant=df.loc[:,[prefix+i+'b' for i in [j for j in food_codebook.keys() if j not in drops]]]
#             if correct48==True:
#                 food_quant.loc[:,[i for i in food_quant.columns if '48' in i]]=food_quant.loc[:,[i for i in food_quant.columns if '48' in i]]*10
#                 food_quant.loc[:,[i for i in food_quant.columns if '49' in i]]=food_quant.loc[:,[i for i in food_quant.columns if '49' in i]]*10
            df=food_quant
        
        if var in ['price','prices']:
            print('Получние датафрейма индивидуальных цен возможно лишь с аргументом contradict=True')
            return None 
    
    if contradict==False:
        if var in ['cost','costs']:
            food_cost=df.loc[:,[prefix+i+'c' for i in [j for j in food_codebook.keys() if j not in drops]]]
            food_quant=df.loc[:,[prefix+i+'b' for i in [j for j in food_codebook.keys() if j not in drops]]]

            df=no_ans_dropper(food_cost,food_quant)
            
        if var in ['quant','quants']: 
            food_cost=df.loc[:,[prefix+i+'c' for i in [j for j in food_codebook.keys() if j not in drops]]]
            food_quant=df.loc[:,[prefix+i+'b' for i in [j for j in food_codebook.keys() if j not in drops]]]
            df=no_ans_dropper(food_quant,food_cost) 
            if correct48==True:
                df.loc[:,[i for i in food_quant.columns if '48' in i]]=df.loc[:,[i for i in food_quant.columns if '48' in i]]*10
                df.loc[:,[i for i in food_quant.columns if '49' in i]]=df.loc[:,[i for i in food_quant.columns if '49' in i]]*10
            
        if var in ['price','prices']:
            food_cost=df.loc[:,[prefix+i+'c' for i in [j for j in food_codebook.keys() if j not in drops]]]
            
            food_cost.rename(columns={i:i[:-1]+'p' for i in food_cost.columns}, inplace=True)
            
            food_quant=df.loc[:,[prefix+i+'b' for i in [j for j in food_codebook.keys() if j not in drops]]]
                
            food_quant.rename(columns={i:i[:-1]+'p' for i in food_quant.columns}, inplace=True)
            
            chisl=no_ans_dropper(food_quant,food_cost) 
            if correct48==True:
                chisl.loc[:,[i for i in food_quant.columns if '48' in i]]=chisl.loc[:,[i for i in food_quant.columns if '48' in i]]*10
                chisl.loc[:,[i for i in food_quant.columns if '49' in i]]=chisl.loc[:,[i for i in food_quant.columns if '49' in i]]*10
            
            food_price=no_ans_dropper(food_cost,food_quant)/chisl 
            food_price.replace([inf, -inf], nan, inplace=True)
            
            df=food_price
        
        
# Рутины переименования столбцов        
    if cut is not None:
        if ('wave' in cut) or ('waves' in cut):
            df=name_cutter(df=df, what='wave')
        if ('quest' in cut) or ('quests' in cut):
            food_cost=name_cutter(df=df, what='quest')
        if ('name' in cut) or ('names' in cut):
            df=food_renamer(df=df)
            
    if add_features is not None:
        exceptions_prefix=['region', 'psu', 'status', 'popul', 'site']
        add_features=[prefix+i if (i not in exceptions_prefix) else i for i in add_features ]
        df=df.join(df_copy[add_features])
        
        if cut is not None:
            if ('wave' in cut) or ('waves' in cut):
                df.rename(columns={i:i[1:] for i in add_features if i not in exceptions_prefix }, inplace=True)
            if ('name' in cut) or ('names' in cut):
                exceptions_prefix_dict={'region': "КОД РЕГИОНА",
                   'psu': "ПЕРВИЧНАЯ ЕДИНИЦА ОТБОРА",
                   'ssu': "ВТОРИЧНАЯ ЕДИНИЦА ОТБОРА", 
                   'status': "ТИП НАСЕЛЕННОГО ПУНКТА",
                   'popul': "ЧИСЛЕННОСТЬ НАСЕЛЕНИЯ",
                   'site': "НОМЕР НАСЕЛЕННОГО ПУНКТА" }
                df.rename(columns={i:exceptions_prefix_dict[i] if i in exceptions_prefix else i[1:] for i in add_features }, inplace=True)
    return df





"""
def food_df(year, variable, cleaned=True,rename_del=['wave','quest']):
    if (year<1994) or (year==1997) or (year==1999):
        return 'Волны {0} года не существует.'.format(year)
    df1=readrlms.read_wave_hh(year)
#=============================================================================    
    food_costs_cols=[]
    for i in df1.columns:
        if 'e1.' in i[1:]:
            if 'c' in i[1:]:
                if '60' not in i[1:]:
                    if '57' not in i[1:]:
                        food_costs_cols.append(i)
    food_costs=df1.loc[:,food_costs_cols]
#=============================================================================          
    food_quant_cols=[]
    for i in df1.columns:
        if 'e1.' in i[1:]:
            if 'b' in i[1:]:
                if '57' not in i[1:]:
                    food_quant_cols.append(i)
    food_quant=df1.loc[:,food_quant_cols]
#=============================================================================
    if 'wave' in rename_del:
        food_costs=renaming_routine(food_costs,'wave')
        food_quant=renaming_routine(food_quant,'wave')
    if 'quest' in rename_del:
        food_costs=renaming_routine(food_costs,'quest')
        food_quant=renaming_routine(food_quant,'quest')
    if rename_del=='names':
        food_costs=renaming_routine_names(food_costs)
        food_quant=renaming_routine_names(food_quant)
#=============================================================================    
    if cleaned==1:
        if variable=='costs' or variable=='cost':
            return cleaning_routine(food_costs,food_quant)
        if variable=='quants' or variable=='quant':
            return cleaning_routine(food_quant,food_costs)
        if variable=='prices' or variable=='price':
            if rename_del=='names':
                result=cleaning_routine(food_costs,food_quant)/cleaning_routine(food_quant,food_costs)
                result.replace([np.inf, -np.inf], np.nan, inplace=True)
                return result
            else:
                if 'quest' not in rename_del:
                    food_costs=renaming_routine(food_costs,'quest')
                    food_quant=renaming_routine(food_quant,'quest')
                    result=cleaning_routine(food_costs,food_quant)/cleaning_routine(food_quant,food_costs)
                    result.replace([np.inf, -np.inf], np.nan, inplace=True)
                    return result
                else:
                    result=cleaning_routine(food_costs,food_quant)/cleaning_routine(food_quant,food_costs)
                    result.replace([np.inf, -np.inf], np.nan, inplace=True)
                    return result
    else: 
        if variable=='costs' or variable=='cost':
            return food_costs
        if variable=='quants' or variable=='quant':
            return food_quant
        if variable=='prices' or variable=='price':
            return 'Невозможно получить индивидуальные цены без чистки данных'
"""



#==========================================================================================
"""
ПЛОТТИНГ (ГРАФИКИ)
"""
#==========================================================================================
"""
2 БАЗОВЫХ ГРАФИКА: region_plot и status_plot
"""
#==========================================================================================
# Можно универсализовать от региона и статуса до любой переменной-индикатора
#==========================================================================================
def region_plot(df, var, prod_num, verbose=False, path=os.getcwd()+'\\RLMS_db', cut='name'):
#     Надо написать для одного продукта, а потом использовать рекурсию, чтобы прописать 'all'
    """
    
    
    Параметры
    ---------
    df : DataFrame, integer, string
        Фрейм данных, год исследования, кодовый символ исследования
    var : string
        Если 'cost' или 'costs', то конечным датафреймом будет фрейм расходов на продукты питания 
        Если 'quant' или 'quants', то конечным датафреймом будет фрейм количеств купленных продуктов питания
        Если 'price' или 'prices', то конечным датафреймом будет фрейм индивидуальных цен продуктов питания
    prod_num : integer, string
        Кодовый номер продукта питания, для которого будет строиться график. 
        Чтобы график был построен для всех продуктов выборки необходимо.
        Если 'all', то генерирует графики соотетствующие всем продуктам в выборке
    verbose: bool, optional
        (default False)
        Если True, то выволит прогресс работы функции.
    path : string, optional
        (default os.getcwd()+'\\RLMS_db')
        Путь на диске к базе данных RLMS.     
    cut: list, string, optional
        (default ['wave', 'quest'])
        Формат названий колонок конечного фрейма данных. 
        Если содержит 'wave' или 'waves', то отрезает от имен колонок код-символ волны.
        Если содержит 'quest' или 'quests', то отрезает от имен колонок код-символ подвопроса. 
        Если равняется 'name' или 'names', то переименует имена колонок в явный формат. 
        Если равняется None, то сохраняет исходные имена колонок. 
    drops : list, optional
        (default ['e1.57','e1.58','e1.59'])
        Не все продукты встречаются в каждом году исследования и потому может иметь смысл исключить из выборки те, что не встречаются в фрейме данных каждой волны RLMS.
    
    add_features : list, optional
        Список кодов (без префикса волны) дополнительных столбцов, которые должны быть в итоговом датафрейме
    """
#   !!!!!Прописать различение вывода (возможно вывод food_df должен включать год)
    df=food_df(df=df, var=var, verbose=verbose, path=os.getcwd()+'\\RLMS_db', add_features=['region'])
    region_dict={}
#   Генерируем словарь, где ключ - это регион, а значение - это список индексов наблюдений, для которых значение region равно ключу. 
    for i in df[df.columns[-1]].cat.categories:
        region_dict[i]=list(df.loc[df[df.columns[-1]] == i].index)
# В РАЗРАБОТКЕ:
    if  prod_num=='all':
        pass
# В РАЗРАБОТКЕ.    

    if prod_num!='all':
        df_prod=df[['e1.'+str(prod_num),'region']]
        plt.figure(figsize=(8, 10), dpi=100)
        num_of_regions=0
        total_number=0
        for reg in region_dict.keys():
            good_index=set(df_prod.dropna().index)&set(region_dict[reg])
            good_index=list(good_index)
            if good_index==[]:
                continue
            plt.scatter(good_index, df_prod.loc[good_index, 'e1.'+str(prod_num)], label=reg+': '+str(len(good_index)),s=20)
            num_of_regions+=1
            total_number+=len(good_index)
        plt.grid()
        plt.ylim(0)
        plt.xlim(0)
#         !!!!!!! Прописать вывод года
        plt.legend(bbox_to_anchor=(1.05, 1),title="Регионы: {} \nЧисло наблюдений: {}".format(num_of_regions,total_number),title_fontsize='large')   
        plt.xlabel('Номер домашнего хозяйства',fontsize='large',fontweight=700)
        pqc_decode={'prices':'Цена за товар',
           'price':'Цена за товар',
           'quant':'Количество товара',
           'quants':'Количество товара',
           'cost':'Затраты на товар',
           'cost':'Затраты на товар'}
        plt.ylabel(str(pqc_decode[var])+'\n'+str(food_codebook['e1.'+str(prod_num)]),fontsize='large',fontweight=700)
#==========================================================================================
def status_plot(df, var):
    pass
#==========================================================================================
def food_plot(df, var):
    pass
#==========================================================================================
"""
АГРЕГАЦИИ ПРОДУКТОВ
"""
#==========================================================================================
food_agregations={'Bondarev':
                 {'Бакалея':[1, 2, 3, 4, 5, 51],
               'Овощи-фрукты': [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 52, 59],
               'Мясо-рыба':[21, 22, 23, 24, 25, 26, 27, 28, 29, 46, 47, 58],
               'Молоко':[30, 31, 32, 33, 34, 35, 36, 38, 39, 45],
               'Кондитерские изделия':[37, 40, 41, 42, 43, 44,49],
               'Алкоголь':[53, 54, 55, 56],
               'Неалкогольные напитки':[48, 50]}
                 }
#==========================================================================================
def food_agregates(df, var, style='Bondarev', including=None, path=os.getcwd()+'\\RLMS_db', drop_zeros=True):
    """
    
    
    Параметры
    ---------    
    df : DataFrame, integer, string
    
    variable : string
        Если 'cost' или 'costs', то конечным датафреймом будет фрейм агрегатов расходов на продукты питания. 
        Если 'share' или 'shares', то конечным датафреймом будет фрейм долей агрегатов расходов в суммарных расходах на продукты питания. 
        Если 'price' или 'prices', то ...
    style : string, optional
        (default 'Bondarev')
        "Стиль" агрегации продкутов питания. 
    verbose : bool, optional
        (default False)
        Если True, то выволит прогресс работы функции.
    path : string, optional
        (default os.getcwd()+'\\RLMS_db')
        Путь на диске к базе данных RLMS.
    drop_zeros : bool, optional
        (default False)
        
    """
    if var in ['price', 'prices']:
        df=food_agregate_prices(df, var='price', style=style)
        return df
    if var in ['cost', 'costs', 'share', 'shares']:
        df=food_df(df=df, var='cost')
    f_agregator=food_agregations[style]
    
    for key in f_agregator.keys():
        df[key]=0
    
    for key in f_agregator.keys():
        for product in f_agregator[key]:
            if 'e1.{}'.format(product) not in list(df.columns):
                continue
            df[key]=df[key]+df['e1.{0}'.format(product)]
    df=df[f_agregator.keys()]

    if drop_zeros==True:
        df=df[-(df.sum(axis=1)==0)]
        df=df[-(df==0).any(axis=1)]
        

    if var in ['share', 'shares']:
#         df['Сумма']=df.sum(axis=1)
#         df=(df.loc[:,df.columns[0]:df.columns[-1]]).div(df.loc[:,df.columns[-1]], axis='index')
        df=df.div(df.sum(axis=1), axis='index')
    if including==None:
        pass
    elif (('total' in including) or ('totals' in including) or (var in ['share', 'shares'])) and (var not in ['price', 'prices']):
        df['Сумма']=df.sum(axis=1)
    
    return df

#==========================================================================================
def food_agregate_prices(df, var='price', style='Bondarev', path=os.getcwd()+'\\RLMS_db'):
    """
    var : 
    alpha alphas
    price prices
    """
    df_agr=food_agregates(df=df, var='cost', style=style, including=None, path=path, drop_zeros=True)
    df_price=food_df(df=df, var='price').loc[df_agr.index]
    df_cost=food_df(df=df, var='cost').loc[df_agr.index]
    df_P=df_agr.copy(deep=True)
    df_shares=df_cost.copy(deep=True)
    
    for agregate in food_agregations[style].keys():
        for prod in food_agregations[style][agregate]:
            if 'e1.{}'.format(prod) not in list(df_cost.columns):
                continue
#             Получаем альфа жи доли продуктов в каждом агрегате
            df_shares.loc[:,'e1.{0}'.format(prod)]=(df_cost.loc[:,'e1.{0}'.format(prod)]).div(df_agr.loc[:,agregate],axis='index')
    if var in ['alpha','alphas']:
        return df_shares
    df_agr_price=(df_shares*df_price).fillna(0)
    
    for agregate in food_agregations[style].keys():
        sum_of_alpha_p=0
        for prod in food_agregations[style][agregate]:
            if 'e1.{}'.format(prod) not in list(df_cost.columns):
                continue
            sum_of_alpha_p=sum_of_alpha_p+df_agr_price.loc[:,'e1.{0}'.format(prod)]
        df_P.loc[:,agregate]=sum_of_alpha_p
    if var in ['price', 'prices']:
        return df_P

#========================================================================================== 
"""
ИНДЕКСЫ ЦЕН
"""
#========================================================================================== 
def stone_price_index(df):
    df_share=food_agregates(df, 'shares')
    df_price=food_agregates(df, 'prices')
    stone=(df_share*log(df_price)).sum(axis=1)
    return stone

def laspeyres_price_index(df):
    df_share=food_agregates(df, 'shares').mean()
    df_price=food_agregates(df, 'prices')
    stone=(df_share*log(df_price)).sum(axis=1)
    return stone

def norm_stone_price_index(df):
    df_share=food_agregates(df, 'shares').mean()
    df_price=food_agregates(df, 'prices')
    stone=(df_share*log(df_price)/df_price.mean()).sum(axis=1)
    return stone


# ПААШЕ!!!!
#========================================================================================== 
"""
ПОЛУЧЕНИЕ МАТРИЦЫ ПЕРЕМЕННЫХ, ПРИГОДНОЙ К AIDS-ОЦЕНКЕ
"""
#========================================================================================== 
def AIDS_matrix(df, price_index='Stone'):
    aids_shares=food_agregates(df=df, var='shares')
    aids_costs=food_agregates(df=df,var='costs')
    aids_prices=log(food_agregates(df=df,var='prices'))
    aids_prices.rename(columns={i:'Цена на '+i for i in aids_prices.columns}, inplace=True)

    if price_index=='Stone':
        price_index=stone_price_index(df)
    elif price_index=='Laspeyres':
        price_index=laspeyres_price_index(df)
    elif price_index=='norm Stone':
        price_index=norm_stone_price_index(df)
    
    aids_resid=pd.DataFrame(log(food_agregates(df,'costs',including=['total']).iloc[:,-1]) - price_index, columns=['Остаток'])
    
    result_df=aids_shares.join(aids_prices).join(aids_resid)
    result_df=add_constant(result_df)
    return result_df
#==========================================================================================
def FAST_AIDS_matrices(df, price_index='Stone', path=os.getcwd()+'\\RLMS_db',verbose=True, add_year=False):
    """
    df : dictionary
    """
    global FAST_AIDS_MATRICES
    FAST_AIDS_MATRICES={}
    for year in df.keys():
        FAST_AIDS_MATRICES[year]=AIDS_matrix(df=df[year], price_index=price_index)
        if add_year==True:
            FAST_AIDS_MATRICES[year][year]=year
        if verbose==True:
            print('Матрица '+str(year)+" построена")
#========================================================================================== 
"""
SUR-ОЦЕНКА AIDS-МОДЕЛИ
"""
#========================================================================================== 
def SUR_AIDS(df, price_index='Stone', style='Bondarev', constrained=False,  homo=True, sym=True, fitted=False):
    """
    df : DataFrame, dictionary
    price_index : string, optional
    "Stone"
    "norm Stone"
    "Laspeyres"
    "Paasche"???
    style : string, optional
    
    """
    if type(df)==dict:
        final_df=pd.concat([df[i] for i in df.keys()], ignore_index=True)
    elif (df.columns==pd.Index(['const', 'Бакалея', 'Овощи-фрукты', 'Мясо-рыба', 'Молоко',
       'Кондитерские изделия', 'Алкоголь', 'Неалкогольные напитки',
       'Цена на Бакалея', 'Цена на Овощи-фрукты', 'Цена на Мясо-рыба',
       'Цена на Молоко', 'Цена на Кондитерские изделия', 'Цена на Алкоголь',
       'Цена на Неалкогольные напитки', 'Остаток'],
      dtype='object')).all():
        final_df=df
    else:
        final_df=AIDS_matrix(df=df, price_index=price_index )
        
    equations = OrderedDict()
# year,fitted=True,cov_type_1='robust',
#               price_index='Stone',constrained=False,
#               out=False,n_neighbors=30,contamination='auto',homo=True, sym=True)

#     Уравнения должны генерироваться  автоматически, не смотря на стиль агрегации
#1
    equations['Бакалея'] = {'dependent': final_df['Бакалея'],
                         'exog': final_df[[ 'const','Цена на Бакалея', 'Цена на Овощи-фрукты', 'Цена на Мясо-рыба','Цена на Молоко', 'Цена на Кондитерские изделия', 'Цена на Алкоголь','Цена на Неалкогольные напитки', 'Остаток']]}
#2
    equations['Овощи-фрукты'] = {'dependent': final_df['Овощи-фрукты'],
                         'exog': final_df[[ 'const','Цена на Бакалея', 'Цена на Овощи-фрукты', 'Цена на Мясо-рыба','Цена на Молоко', 'Цена на Кондитерские изделия', 'Цена на Алкоголь','Цена на Неалкогольные напитки', 'Остаток']]}
#3
    equations['Мясо-рыба'] = {'dependent': final_df["Мясо-рыба"],
                         'exog': final_df[[ 'const','Цена на Бакалея', 'Цена на Овощи-фрукты', 'Цена на Мясо-рыба','Цена на Молоко', 'Цена на Кондитерские изделия', 'Цена на Алкоголь','Цена на Неалкогольные напитки', 'Остаток']]}
#4
    equations['Молоко'] = {'dependent': final_df["Молоко"],
                         'exog': final_df[[ 'const','Цена на Бакалея', 'Цена на Овощи-фрукты', 'Цена на Мясо-рыба','Цена на Молоко', 'Цена на Кондитерские изделия', 'Цена на Алкоголь','Цена на Неалкогольные напитки', 'Остаток']]}
#5
    equations['Кондитерские изделия'] = {'dependent': final_df["Кондитерские изделия"],
                         'exog': final_df[[ 'const','Цена на Бакалея', 'Цена на Овощи-фрукты', 'Цена на Мясо-рыба','Цена на Молоко', 'Цена на Кондитерские изделия', 'Цена на Алкоголь','Цена на Неалкогольные напитки', 'Остаток']]}
#6
    equations['Алкоголь'] = {'dependent': final_df["Алкоголь"],
                       'exog': final_df[[ 'const','Цена на Бакалея', 'Цена на Овощи-фрукты', 'Цена на Мясо-рыба','Цена на Молоко', 'Цена на Кондитерские изделия', 'Цена на Алкоголь','Цена на Неалкогольные напитки', 'Остаток']]}
# #7
#     equations['Неалкогольные напитки'] = {'dependent': final_df["Неалкогольные напитки"],
#                       'exog': final_df[[ 'const','Цена на Бакалея', 'Цена на Овощи-фрукты', 'Цена на Мясо-рыба','Цена на Молоко', 'Цена на Кондитерские изделия', 'Цена на Алкоголь','Цена на Неалкогольные напитки', 'Остаток']]}
   
    if constrained==True:
        if (homo==True) and (sym==True):
            mod=SUR(equations)
            mod.add_constraints(both_r)
        else:
            if homo==True:
                mod=SUR(equations)
                mod.add_constraints(homo_r)
            if sym==True:
                mod=SUR(equations)
                mod.add_constraints(sym_r)
        
        if fitted==False:
            return mod
        if fitted==True:
            modfit=mod.fit(iterate=True)
            return modfit
        
    if fitted==True:
        mod=SUR(equations)
        modfit=mod.fit(iterate=True)
        return modfit

    if fitted==False:
        return SUR(equations)

"""
readrlms.FAST_HH_DFS[2021]
"""

"""
import RLMS_HSE.AIDS as aids
from importlib import reload  # Python 3.4+
aids = reload(aids)
"""
#========================================================================================== 
"""
ГЕНЕРАЦИЯ ОГРАНИЧЕНИЙ
"""
#==========================================================================================
Bondarev_param_names=['Бакалея_const',
 'Бакалея_Цена на Бакалея',
 'Бакалея_Цена на Овощи-фрукты',
 'Бакалея_Цена на Мясо-рыба',
 'Бакалея_Цена на Молоко',
 'Бакалея_Цена на Кондитерские изделия',
 'Бакалея_Цена на Алкоголь',
 'Бакалея_Цена на Неалкогольные напитки',
 'Бакалея_Остаток',
 'Овощи-фрукты_const',
 'Овощи-фрукты_Цена на Бакалея',
 'Овощи-фрукты_Цена на Овощи-фрукты',
 'Овощи-фрукты_Цена на Мясо-рыба',
 'Овощи-фрукты_Цена на Молоко',
 'Овощи-фрукты_Цена на Кондитерские изделия',
 'Овощи-фрукты_Цена на Алкоголь',
 'Овощи-фрукты_Цена на Неалкогольные напитки',
 'Овощи-фрукты_Остаток',
 'Мясо-рыба_const',
 'Мясо-рыба_Цена на Бакалея',
 'Мясо-рыба_Цена на Овощи-фрукты',
 'Мясо-рыба_Цена на Мясо-рыба',
 'Мясо-рыба_Цена на Молоко',
 'Мясо-рыба_Цена на Кондитерские изделия',
 'Мясо-рыба_Цена на Алкоголь',
 'Мясо-рыба_Цена на Неалкогольные напитки',
 'Мясо-рыба_Остаток',
 'Молоко_const',
 'Молоко_Цена на Бакалея',
 'Молоко_Цена на Овощи-фрукты',
 'Молоко_Цена на Мясо-рыба',
 'Молоко_Цена на Молоко',
 'Молоко_Цена на Кондитерские изделия',
 'Молоко_Цена на Алкоголь',
 'Молоко_Цена на Неалкогольные напитки',
 'Молоко_Остаток',
 'Кондитерские изделия_const',
 'Кондитерские изделия_Цена на Бакалея',
 'Кондитерские изделия_Цена на Овощи-фрукты',
 'Кондитерские изделия_Цена на Мясо-рыба',
 'Кондитерские изделия_Цена на Молоко',
 'Кондитерские изделия_Цена на Кондитерские изделия',
 'Кондитерские изделия_Цена на Алкоголь',
 'Кондитерские изделия_Цена на Неалкогольные напитки',
 'Кондитерские изделия_Остаток',
 'Алкоголь_const',
 'Алкоголь_Цена на Бакалея',
 'Алкоголь_Цена на Овощи-фрукты',
 'Алкоголь_Цена на Мясо-рыба',
 'Алкоголь_Цена на Молоко',
 'Алкоголь_Цена на Кондитерские изделия',
 'Алкоголь_Цена на Алкоголь',
 'Алкоголь_Цена на Неалкогольные напитки',
 'Алкоголь_Остаток']
#==========================================================================================
homo = pd.DataFrame(
    columns=Bondarev_param_names,
    index=["rest{0}".format(i) for i in range(6)],
    dtype=float64)
homo_r=homo.fillna(0)
homo_r.loc['rest0',['Бакалея_Цена на Бакалея','Бакалея_Цена на Овощи-фрукты', 'Бакалея_Цена на Мясо-рыба','Бакалея_Цена на Молоко', 'Бакалея_Цена на Кондитерские изделия','Бакалея_Цена на Алкоголь', 'Бакалея_Цена на Неалкогольные напитки']]=1
homo_r.loc['rest1',['Овощи-фрукты_Цена на Бакалея','Овощи-фрукты_Цена на Овощи-фрукты', 'Овощи-фрукты_Цена на Мясо-рыба','Овощи-фрукты_Цена на Молоко','Овощи-фрукты_Цена на Кондитерские изделия','Овощи-фрукты_Цена на Алкоголь','Овощи-фрукты_Цена на Неалкогольные напитки']]=1     
homo_r.loc['rest2',['Мясо-рыба_Цена на Бакалея','Мясо-рыба_Цена на Овощи-фрукты', 'Мясо-рыба_Цена на Мясо-рыба','Мясо-рыба_Цена на Молоко', 'Мясо-рыба_Цена на Кондитерские изделия','Мясо-рыба_Цена на Алкоголь', 'Мясо-рыба_Цена на Неалкогольные напитки']]=1       
homo_r.loc['rest3',['Молоко_Цена на Бакалея','Молоко_Цена на Овощи-фрукты', 'Молоко_Цена на Мясо-рыба','Молоко_Цена на Молоко', 'Молоко_Цена на Кондитерские изделия','Молоко_Цена на Алкоголь', 'Молоко_Цена на Неалкогольные напитки']]=1             
homo_r.loc['rest4',['Кондитерские изделия_Цена на Бакалея','Кондитерские изделия_Цена на Овощи-фрукты','Кондитерские изделия_Цена на Мясо-рыба','Кондитерские изделия_Цена на Молоко','Кондитерские изделия_Цена на Кондитерские изделия','Кондитерские изделия_Цена на Алкоголь','Кондитерские изделия_Цена на Неалкогольные напитки']]=1              
homo_r.loc['rest5',['Алкоголь_Цена на Бакалея', 'Алкоголь_Цена на Овощи-фрукты','Алкоголь_Цена на Мясо-рыба', 'Алкоголь_Цена на Молоко','Алкоголь_Цена на Кондитерские изделия', 'Алкоголь_Цена на Алкоголь','Алкоголь_Цена на Неалкогольные напитки']]=1   
# homo_r.loc['rest6',['Неалкогольные напитки_Цена на Бакалея', 'Неалкогольные напитки_Цена на Овощи-фрукты','Неалкогольные напитки_Цена на Мясо-рыба', 'Неалкогольные напитки_Цена на Молоко','Неалкогольные напитки_Цена на Кондитерские изделия', 'Неалкогольные напитки_Цена на Алкоголь','Неалкогольные напитки_Цена на Неалкогольные напитки']]=1   

#==========================================================================================
r=0
r = pd.DataFrame(
    columns=Bondarev_param_names,
    index=["rest{0}".format(i) for i in range(15)],
    dtype=float64)
r=r.fillna(0)
r.loc['rest0',['Бакалея_Цена на Овощи-фрукты', 'Овощи-фрукты_Цена на Бакалея']]=[1,-1]
r.loc['rest1',['Бакалея_Цена на Мясо-рыба', 'Мясо-рыба_Цена на Бакалея']]=[1,-1]
r.loc['rest2',['Бакалея_Цена на Молоко', 'Молоко_Цена на Бакалея']]=[1,-1]
r.loc['rest3',['Бакалея_Цена на Кондитерские изделия', 'Кондитерские изделия_Цена на Бакалея']]=[1,-1]
r.loc['rest4',['Бакалея_Цена на Алкоголь', 'Алкоголь_Цена на Бакалея']]=[1,-1]
r.loc['rest5',['Овощи-фрукты_Цена на Мясо-рыба', 'Мясо-рыба_Цена на Овощи-фрукты']]=[1,-1]
r.loc['rest6',['Овощи-фрукты_Цена на Молоко', 'Молоко_Цена на Овощи-фрукты']]=[1,-1]
r.loc['rest7',['Овощи-фрукты_Цена на Кондитерские изделия', 'Кондитерские изделия_Цена на Овощи-фрукты']]=[1,-1]
r.loc['rest8',['Овощи-фрукты_Цена на Алкоголь', 'Алкоголь_Цена на Овощи-фрукты']]=[1,-1]
r.loc['rest9',['Молоко_Цена на Мясо-рыба', 'Мясо-рыба_Цена на Молоко']]=[1,-1]
r.loc['rest10',['Молоко_Цена на Кондитерские изделия', 'Кондитерские изделия_Цена на Молоко']]=[1,-1]
r.loc['rest11',['Молоко_Цена на Алкоголь', 'Алкоголь_Цена на Молоко']]=[1,-1]
r.loc['rest12',['Мясо-рыба_Цена на Кондитерские изделия', 'Кондитерские изделия_Цена на Мясо-рыба']]=[1,-1]
r.loc['rest13',['Мясо-рыба_Цена на Алкоголь', 'Алкоголь_Цена на Мясо-рыба']]=[1,-1]
r.loc['rest14',['Кондитерские изделия_Цена на Алкоголь', 'Алкоголь_Цена на Кондитерские изделия']]=[1,-1]
sym_r=r
sym_r
#==========================================================================================
homo_r_sym=homo_r.copy(deep=1)
sym_homo_r=sym_r.copy(deep=1)
homo_r_sym.index=['rest{0}'.format(k) for k in range(15,21)]
both_r=sym_homo_r.append(homo_r_sym)
#==========================================================================================




#==========================================================================================
# ЛЕГАСИ-ФУНКЦИИ
    
"""
def FAST_food_agregate_price(year,style_of_agregation='Bondarev',good_price_names=False):
    if style_of_agregation=='Bondarev':
        agr_dict=Bondarev_dict
    f_food=FAST_food_df(year, 'costs',cleaned=True).loc[(FAST_food_agregates(year, "costs",del_zero=1).index)]
    f_price=FAST_food_df(year, 'prices',cleaned=True).loc[(FAST_food_agregates(year, "costs",del_zero=1).index)]
    f_agr=FAST_food_agregates(year, "costs",del_zero=1)
    f_agr_P=f_agr.copy(deep=1)
    f_shares=f_food.copy(deep=1)
    for i in agr_dict.keys():
        for j in agr_dict[i]:
            if 'e1.{}'.format(j) not in list(f_food.columns):
                continue
            f_shares.loc[:,'e1.{0}'.format(j)]=(f_food.loc[:,'e1.{0}'.format(j)]).div(f_agr.loc[:,i],axis='index')
    f_agr_price=(f_shares*f_price).fillna(0)
    for i in agr_dict.keys():
        q=0
        for j in agr_dict[i]:
            if 'e1.{}'.format(j) not in list(f_food.columns):
                continue
            q=q+f_agr_price.loc[:,'e1.{0}'.format(j)]
        f_agr_P.loc[:,i]=q
    if good_price_names==True:
        for i in f_agr_P.columns:
            f_agr_P=f_agr_P.rename({i : 'Цена на {0}'.format(i)},axis=1)
    return f_agr_P
"""

#==========================================================================================
"""
ПРОВЕРКА КАЧЕСТВА ПРОДУКТОВ
"""
#==========================================================================================
def food_regressions_check(df, drops=['e1.57','e1.58','e1.59'], outlier_cleaning=False, outlier_threshhold=0.05, verbose=False, st=False, return_dfs=False, agregates=False, dummies=['status', 'region', 'd7']):
    """
    """
    fin_dict={}
    if agregates==False:
        food_cols=[i for i in food_codebook.keys() if i not in drops]
        f_df=food_df(df, 'price', drops=drops)
    if agregates==True:
        food_cols=food_agregations['Bondarev'].keys()
        f_df=food_agregates(df, 'price')
    
    for col in food_cols:
        df_i=(pd.DataFrame(f_df[col]).dropna()).join((full_preprocessing(df, ['f14'])*7/30)).dropna()
#         df_i['f14_2']=df_i['f14']**2
        if 'status' in dummies:
            df_i=df_i.join(pd.get_dummies(full_preprocessing(df, ['status']).dropna(), drop_first=True))
#             df_i=df_i.join(full_preprocessing(df, ['status']).dropna())
        if 'region' in dummies:
            df_i=df_i.join(pd.get_dummies(full_preprocessing(df, ['region']).dropna(), drop_first=True))
#             df_i=df_i.join(full_preprocessing(df, ['region']).dropna())
        if 'd7' in dummies:
            df_d7=full_preprocessing(df, ['d7'])
            df_d7['d7']=df_d7['d7'].cat.rename_categories(lambda x: x[1:] if x[0]==' ' else x)
#             df_d7=df_d7.fillna('Нет').dropna()
#             df_i=df_i.join(df_d7).dropna()
            df_i=df_i.join(pd.get_dummies(df_d7.fillna('Нет').dropna(), drop_first=True)).dropna()
        
        if len(df_i)<=2:
            fin_dict[col]=nan
            continue
        if st==True:
            df_i.iloc[:,0:2]=df_i.iloc[:,0:].apply(stats.zscore)
        if return_dfs==True and outlier_cleaning==False:
            fin_dict[col]=df_i
            continue
        y=df_i.iloc[:,0]
        X=df_i.iloc[:,1:]
        X=add_constant(pd.DataFrame(X))
        model = OLS(y, X)
        results = model.fit(cov_type='HC3')
#         model=ols(formula=f'Q("{col}")~f14+C(status)+C(region)+C(d7)'.format(),data=df_i)
#         results = model.fit()
#   В разработке      
#         if regularization==True:
#             return model.fit_regularized(alpha=alpha) 
#         if regularization==False:
            
            
            
            
        if outlier_cleaning==True:
            outliers=results.outlier_test()
            nonoutliers=outliers[outliers['bonf(p)']>=outlier_threshhold].index
            if return_dfs==True:
                df_i=df_i.iloc[df_i.index.isin(nonoutliers)]
                fin_dict[col]=df_i
                continue
            y=y.iloc[y.index.isin(nonoutliers)]
            X=X.iloc[X.index.isin(nonoutliers)]
            model = OLS(y, X)
#             df_i=pd.DataFrame(y).join(X)
#             model=ols(formula=f'Q("{col}")~f14+C(status)+C(region)+C(d7)',data=df_i)
            results = model.fit(cov_type='HC3')
            if verbose==True:
                print(f'Читска от выбросов для {col} закончена.')
        fin_dict[col]=results
        if verbose==True:
            print(f'Оценки для {col} получены.')
    return fin_dict
#==========================================================================================
def food_quality_check(df, drops=['e1.57','e1.58','e1.59'], metric='p_value', agregates=False, dummies=['status', 'region', 'd7'], outlier_cleaning=False, outlier_threshhold=0.05, verbose=False, st=False):
    """
    """
    regr_dict=food_regressions_check(df=df, drops=drops, agregates=agregates, dummies=dummies, outlier_cleaning=outlier_cleaning, outlier_threshhold=outlier_threshhold, verbose=verbose, st=st)
    if metric=='p_value':
        fin_dict={i:regr_dict[i].pvalues['f14'] for i in regr_dict.keys()}
    return fin_dict

        

"""
readrlms.FAST_HH_DFS[2021]
"""

"""
import RLMS_HSE.AIDS as aids
from importlib import reload  # Python 3.4+
aids = reload(aids)
"""