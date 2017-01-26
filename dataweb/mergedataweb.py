# -*- coding: utf-8 -*-
"""
Gestión de datos recogidos en web de forma periódica
@author: Eugenio Panadero
"""
import pandas as pd
from pandas.tseries.offsets import Day


__author__ = 'Eugenio Panadero'
__copyright__ = "Copyright 2015, AzogueLabs"
__credits__ = ["Eugenio Panadero"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Eugenio Panadero"


def pdmerge_respeta_tz(func_merge, tz_ant=None, *args, **kwargs):
    """
    Programación defensiva por issue: pandas BUG (a veces, el index pierde el tz):
        - issue #7795: concat of objects with the same timezone get reset to UTC;
        - issue #10567: DataFrame combine_first() loses timezone information for datetime columns
            https://github.com/pydata/pandas/issues/10567
    :param tz_ant: TZ de referencia para establecerlo al final si se ha perdido en la operación
    :param func_merge: puntero a función que realiza la operación de merge / join / combine / etc.
    :param args: argumentos para func_merge
    :param kwargs: argumentos para func_merge
    :return: dataframe_merged con TZ anterior
    """
    df_merged = func_merge(*args, **kwargs)
    if tz_ant is not None and tz_ant != df_merged.index.tz:
        # print_warn('Error pandas: join prevProg + demandaGeneracion pierde timezone (%s->%s)'
        #            % (data_import[KEYS_DATA[0]].index.tz, tz_ant))
        df_merged.index = df_merged.index.tz_convert(tz_ant)
    return df_merged


# noinspection PyTypeChecker
def merge_data(lista_dfs_o_dict, keys_merge=None):
    """
    Realiza y devuelve el merge de una lista de pandas DataFrame's (o bien de un diccionario de {key:pd.Dataframe}).
    Coge la primera y en ella va añadiendo el resto, de una en una.
    Seguramente no sea la mejor opción para realizar la fusión de los datos de distintos días, pero aguanta bien la
    superposición de muestras (mejor que hacer un concat(dataframes)).
    Aun cuando las DF's individuales estén localizadas (index con TZ), en ocasiones el merge aparece en UTC y requiere
    una operación de conversión a TZ: data_a_corregir.set_index(data_a_corregir.index.tz_convert(tz), inplace=True)
    :param lista_dfs_o_dict:
    :param keys_merge: (OPC)
    """

    def _merge_lista(lista_dfs):
        if len(lista_dfs) == 2 and lista_dfs[0].index[-1] == lista_dfs[1].index[-1]:
            df0 = lista_dfs[0]
            df0.update(lista_dfs[1])
        elif ((all([len(df_i) == 1 for df_i in lista_dfs])) or
                (type(lista_dfs[0].index.freq) is Day and len(lista_dfs_o_dict) > 2)):
            df0 = pd.DataFrame(pd.concat(lista_dfs))
            if lista_dfs[0].index.freq and df0.index.freq is None:
                df0.index.freq = lista_dfs[0].index.freq
        else:
            df0 = lista_dfs[0]
            for df1 in lista_dfs[1:]:
                df0 = df0.combine_first(df1)
        return df0

    if len(lista_dfs_o_dict) > 0 and type(lista_dfs_o_dict[0]) is dict:
        if keys_merge is None:
            keys_merge = lista_dfs_o_dict[0].keys()
        dict_merge = dict()
        for k in keys_merge:
            lista_k = sorted([d[k] for d in lista_dfs_o_dict if (d is not None) and (d[k] is not None)],
                             key=lambda item: item.index[0])
            try:
                dict_merge[k] = pdmerge_respeta_tz(_merge_lista, lista_k[0].index.tz, lista_k)
            except AttributeError:
                print('ERROR!')
                dict_merge[k] = pdmerge_respeta_tz(_merge_lista, None, lista_k)
        return dict_merge
    elif len(lista_dfs_o_dict) > 0:
        try:
            lista_merge = sorted(lista_dfs_o_dict, key=lambda item: item.index[0])
            return pdmerge_respeta_tz(_merge_lista, lista_merge[0].index.tz, lista_merge)
        except AttributeError:
            lista_dfs_o_dict = [l for l in lista_dfs_o_dict if l is not None]
            if len(lista_dfs_o_dict) > 0:
                lista_merge = sorted(lista_dfs_o_dict, key=lambda item: item.index[0])
                return pdmerge_respeta_tz(_merge_lista, lista_merge[0].index.tz, lista_merge)
        except TypeError as e:
            print(e, e.__class__)
            print(lista_dfs_o_dict)
    return None
