# -*- coding: utf-8 -*-
"""
Gestión de datos recogidos en web de forma periódica
@author: Eugenio Panadero
"""
__author__ = 'Eugenio Panadero'
__copyright__ = "Copyright 2015, AzogueLabs"
__credits__ = ["Eugenio Panadero"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Eugenio Panadero"

import datetime as dt
import os

import numpy as np
import pandas as pd
import pytz

# Colored output
from prettyprinting import print_warn, print_err, print_ok, print_info, print_secc, print_bold

from dataweb.requestweb.requestweb import get_data_en_intervalo
from dataweb.mergedataweb import merge_data
from dataweb.requestweb.requestweb import USAR_MULTITHREAD, NUM_RETRIES, TIMEOUT, DATE_FMT, MAX_THREADS_REQUESTS

# Variables por defecto
KEY_DATA = 'data'
TZ = pytz.timezone('Europe/Madrid')
DATE_INI = '2015-01-01'

# Pandas output display config:
# pd.set_option('display.height', 1000)
pd.set_option('display.max_rows', 75)
pd.set_option('display.max_columns', 25)
pd.set_option('display.width', 240)  # 4x para pantalla ancha


class DataWeb(object):
    """
    Superclase DataWeb: Clase instanciable para asumir la gestión del respaldo local de una base de datos accesible
    vía web con actualizaciones diarias.
    El histórico de datos desde la fecha de inicio suministrada se gestiona como una o varias Pandas DataFrame's
    y se guarda en disco en formato HDF5. La base de datos se actualiza hasta el instante actual en cada ejecución,
    y también proporciona interfaces sencillas para la gestión de dicha base.
    Está preparada para datos indexados por tiempo localizado, para gestionar adecuadamente los cambios de hora (DST).
    ** Requiere únicamente implementar las funciones 'func_urls_data_dia' y 'func_procesa_data_dia' en la clase hija,
    junto a los parámetros típicos para el archivo web implementado: 'path_database', 'DATE_INI', 'DATE_FMT', 'TZ',
    'TS_DATA' (periodo de muestreo de las entradas), etc.
    La configuración de los requests a web se realiza mediante 'NUM_RETRIES', 'TIMEOUT' y 'MAX_THREADS_REQUESTS'.
    Este último debe tunearse adecuadamente en función del servidor a acceder, con tal de no saturarlo.
    """
    def __init__(self,
                 path_database='store.h5',
                 titulo='Datos de: WEB XXXXXX',
                 forzar_update=False, verbose=True,
                 **kwargs):
        # Init
        self.PATH_DATABASE = os.path.abspath(path_database)
        if not os.path.exists(self.PATH_DATABASE):
            forzar_update = True
        self.store = pd.HDFStore(self.PATH_DATABASE)
        self.titulo = titulo
        self.verbose = verbose
        self.data = dict()
        self.update_init, self.USAR_MULTITHREAD = True, USAR_MULTITHREAD
        # Limita el nº de días a actualizar de golpe (útil en las etapas iniciales de implementación)
        self.MAX_ACT_EXEC = 1000000
        self.TZ = TZ
        self.DATE_FMT, self.DATE_INI, self.DATE_FIN = DATE_FMT, DATE_INI, None
        self.NUM_TS_MIN_PARA_ACT = 1  # 1 entrada nueva para actualizar
        self.TS_DATA = 600  # Muestreo en segs.(1h)
        self.NUM_RETRIES, self.TIMEOUT, self.MAX_THREADS_REQUESTS = NUM_RETRIES, TIMEOUT, MAX_THREADS_REQUESTS
        # Re-definición de parámetros desde arriba
        self.keys_attrs = [k for k in kwargs]
        [setattr(self, k, kwargs[k]) for k in self.keys_attrs]

        # Identificadores para múltiples DataFrames
        if 'keys_data_web' in self.keys_attrs:
            self.masterkey = self.keys_data_web[0]
        else:
            self.masterkey = KEY_DATA
            self.keys_data_web = [KEY_DATA]

        if self.update_init:  # Inicia / update de la base de datos
            self.update_data(forzar_update)
        else:
            self.load_data()

        self.printif('\nINFORMACIÓN EN BASE DE DATOS:', 'info')
        # [self.info_data(data[k],verbose=self.verbose) for k in data.keys()]
        self.printif(self.store, 'info')

    # you want to override this on the child classes
    def url_data_dia(self, key_dia):
        # url = func_url_data_dia(key_dia)
        raise NotImplementedError

    # you want to override this on the child classes
    def procesa_data_dia(self, key_dia, response):
        # data_import, ok = func_procesa_data_dia(key_dia, response)
        raise NotImplementedError

    def __get_data_en_intervalo(self, d0=None, df=None):
        """
        Obtiene los datos en bruto de la red realizando múltiples requests al tiempo
        Procesa los datos en bruto obtenidos de la red convirtiendo a Pandas DataFrame
        """

        params = {'date_fmt': self.DATE_FMT,
                  'usar_multithread': self.USAR_MULTITHREAD, 'max_threads_requests': self.MAX_THREADS_REQUESTS,
                  'timeout': self.TIMEOUT, 'num_retries': self.NUM_RETRIES,
                  'func_procesa_data_dia': self.procesa_data_dia, 'func_url_data_dia': self.url_data_dia,
                  'max_act_exec': self.MAX_ACT_EXEC}
        data_get, hay_errores, str_import = get_data_en_intervalo(d0, df, **params)
        if not hay_errores:
            self.integridad_data(data_get)
            self.printif(str_import, 'ok')
            if type(data_get) is pd.DataFrame:
                data_get = {self.masterkey: data_get}
            return data_get
        else:
            return None

    def last_entry(self, data_revisar=None, key_revisar=None):
        """
        Obtiene el Timestamp del último valor de la base de datos seleecionada,
        junto con el nº de entradas (filas) total de dicho paquete de datos.

        :param data_revisar (OPC): Se puede pasar un dataframe específico
        :param key_revisar (OPC): Normalmente, para utilizar 'dem'
        :return: tmax, num_entradas
        """
        key_revisar = key_revisar or self.masterkey
        data_revisar = self.data if data_revisar is None else data_revisar
        # return tmax, num_entradas
        if key_revisar in data_revisar.keys():
            data_rev = data_revisar[key_revisar]
            return data_rev.index[-1].to_datetime(), len(data_rev)
        else:
            return pd.Timestamp(dt.datetime.strptime(self.DATE_INI, self.DATE_FMT), tz=self.TZ).to_datetime(), 0

    def printif(self, obj_print, tipo_print=None):
        if self.verbose:
            if tipo_print is None:
                print(obj_print)
            elif tipo_print == 'secc':
                print_secc(obj_print)
            elif tipo_print == 'ok':
                print_ok(obj_print)
            elif tipo_print == 'info':
                print_info(obj_print)
            elif tipo_print == 'error':
                print_err(obj_print)
            elif tipo_print == 'warning':
                print_warn(obj_print)
            elif tipo_print == 'bold':
                print_bold(obj_print)
            else:
                print_err(obj_print)
                assert()

    def __actualiza_datos(self, data_ant=None, tmax=None):
        data_act, hay_nueva_info = None, False
        if data_ant is None:
            data_act = self.__get_data_en_intervalo(self.DATE_INI, self.DATE_FIN)
            if data_act:
                hay_nueva_info = True
        else:
            data_act = data_ant
            now = dt.datetime.now(tz=self.TZ)
            delta = int(np.ceil((now - tmax).total_seconds() / self.TS_DATA))
            if delta > self.NUM_TS_MIN_PARA_ACT:
                d0, df = tmax.date(), now.date()
                data_new = self.__get_data_en_intervalo(d0, df)
                if data_new:
                    tmax_new, num_entradas = self.last_entry(data_new)
                    self.printif('* INFORMACIÓN ADQUIRIDA: %lu valores, hasta %s'
                                 % (num_entradas, tmax_new.strftime(self.DATE_FMT)), 'ok')
                    # self.info_data(data_new, verbose=self.verbose)
                    data_act = merge_data([data_ant, data_new], self.keys_data_web)
                    hay_nueva_info = True
            else:
                self.printif('LA INFORMACIÓN ESTÁ ACTUALIZADA (delta = %.1f segs)' % (now - tmax).total_seconds(), 'ok')
        return data_act, hay_nueva_info

    def update_data(self, forzar_update=False):
        # Check/Lectura de base de datos hdf en disco (local)
        try:
            if forzar_update:
                self.printif('Se procede a actualizar TODOS los datos (force update ON)', 'info')
                assert ()
            self.load_data()
            tmax, num_entradas = self.last_entry()
            if num_entradas > 0:
                data_ant = self.data
                self.printif('''* BASE DE DATOS LOCAL HDF:\n\tNº entradas:\t%lu mediciones\n\tÚltima:     \t%s'''
                             % (num_entradas, tmax.strftime('%d-%m-%Y %H:%M')), 'info')
            else:
                data_ant = None
        except Exception as e:
            if not forzar_update:
                print_warn('  --> NO SE PUEDE LEER LA BASE DE DATOS LOCAL HDF (Excepción leyendo: %s:%s)'
                           % (type(e).__name__, str(e)))
                self.printif('  --> Se procede a realizar la captura de TODOS los datos existentes:', 'warning')
            data_ant, tmax = None, None
        # Actualización de la base de datos
        data_act, hay_nueva_info = self.__actualiza_datos(data_ant, tmax)
        self.data = data_act
        if hay_nueva_info:  # Grabación de la base de datos hdf en disco (local)
            self.save_data()
            tmax, num_entradas = self.last_entry()
            self.printif('''\tNº entradas:\t%lu mediciones\n\tÚltima:     \t%s'''
                         % (num_entradas, tmax.strftime('%d-%m-%Y %H:%M')), 'ok')

    def integridad_data(self, data_integr=None, key=None):
        """
        Comprueba que el index de cada dataframe de la base de datos sea de fechas, único (sin duplicados) y creciente
        """
        def _assert_integridad(df):
            assert(df.index.is_unique and df.index.is_monotonic_increasing and df.index.is_all_dates)

        if data_integr is None:
            data_integr = self.data
        if type(data_integr) is dict:
            if key is None:
                keys = data_integr.keys()
            else:
                keys = [key]
            [_assert_integridad(data_integr[k]) for k in keys]
        else:
            _assert_integridad(data_integr)

    def info_data(self, data_info=None, completo=True, key=None, verbose=True):

        def _info_dataframe(data_frame):
            if completo:
                print('\n', data_frame.info(), '\n', data_frame.describe(), '\n')
            print(data_frame.head())
            print(data_frame.tail())

        if verbose:
            if data_info is None:
                _info_dataframe(self.data[key or self.masterkey])
            elif type(data_info) is dict:
                [_info_dataframe(df) for df in data_info.values()]
            else:
                _info_dataframe(data_info)

    def save_data(self, dataframe=None, key_data=None):
        """ Guarda en disco la información"""

        def _save_data_en_key(store, key_save, data_save):
            # print(SALVANDO INFO en key=%s:' % key)
            try:
                store.put(key_save, data_save, format='table', mode='w')
            except TypeError as e:
                print_err('ERROR SALVANDO INFO: %s' % str(e))
                print_warn(key_save)
                print_warn(data_save)

        assert self.store.is_open
        self.integridad_data()
        if dataframe is None:
            if key_data is None:
                for k in self.data.keys():
                    _save_data_en_key(self.store, k, self.data[k])
            else:
                _save_data_en_key(self.store, key_data, self.data[key_data])
        else:
            _save_data_en_key(self.store, key_data or self.masterkey, dataframe)
        self.store.close()
        self.store.open()

    def load_data(self, key=None, **kwargs):
        """ Lee de disco la información y la devuelve"""
        assert self.store.is_open
        # self.printif('BASE DE DATOS EN [%s]: %lu tabla(s):' % (self.PATH_DATABASE, len(self.store.keys())))
        # if self.verbose:
        #     for k in self.store.keys():
        #         print('CLAVE: %sTIPO: %s' % (k.ljust(20), self.store.get_storer(k)))
        if key:
            return pd.read_hdf(self.PATH_DATABASE, key, **kwargs)
        else:
            data_load = dict()
            for k in self.store.keys():
                k = k.replace('/', '')
                # **kwargs ej:= where=['index > 2009','index < 2010'],columns=['ordinal']
                data_load[k] = pd.read_hdf(self.PATH_DATABASE, k, **kwargs)
            self.data = data_load
        self.integridad_data()

    def append_delta_index(self, ts_data=None, data_delta=None, key=KEY_DATA):
        if data_delta is None:
            data_delta = self.data[key]
            reasign = True
        else:
            reasign = False
        idx_utc = data_delta.index.tz_convert('UTC')
        tt = idx_utc.values
        delta = tt[1:] - tt[:-1]
        if ts_data is None:
            delta /= 1e9  # pasa a segs
            data_delta['delta'] = pd.Series(data=delta, index=data_delta.index[1:])
            data_delta.delta.fillna(1, inplace=True)
        else:
            delta.dtype = np.int64
            delta /= 1e9 * ts_data
            data_delta['delta_T'] = pd.Series(data=delta, index=data_delta.index[1:])
            data_delta.delta_T.fillna(1, inplace=True)
        if reasign:
            self.data[key] = data_delta
        else:
            return data_delta

    def close(self):
        self.store.close()
