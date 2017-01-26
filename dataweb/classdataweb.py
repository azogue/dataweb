# -*- coding: utf-8 -*-
"""
Gestión de datos recogidos en web de forma periódica
@author: Eugenio Panadero
"""
import datetime as dt
import logging
import os

import numpy as np
import pandas as pd
import pytz
from dataweb.mergedataweb import merge_data
from dataweb.requestweb import USAR_MULTITHREAD, NUM_RETRIES, TIMEOUT, DATE_FMT, MAX_THREADS_REQUESTS
from dataweb.requestweb import get_data_en_intervalo
from prettyprinting import print_warn, print_err, print_ok, print_info, print_secc, print_bold


__author__ = 'Eugenio Panadero'
__copyright__ = "Copyright 2015, AzogueLabs"
__credits__ = ["Eugenio Panadero"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Eugenio Panadero"

# Variables por defecto
KEY_DATA = 'data'
TZ = pytz.timezone('Europe/Madrid')
DATE_INI = '2015-01-01'

# Pandas output display config:
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
                 forzar_update=False, verbose=True, update_init=True,
                 **kwargs):
        # Init
        self.PATH_DATABASE = os.path.abspath(path_database)
        if not os.path.exists(self.PATH_DATABASE):
            forzar_update = True
        self.store = pd.HDFStore(self.PATH_DATABASE)
        self.store.close()
        self.titulo = titulo
        self.verbose = verbose
        self.data = dict()
        self.update_init, self.USAR_MULTITHREAD = update_init, USAR_MULTITHREAD
        # Limita el nº de días a actualizar de golpe (útil en las etapas iniciales de implementación)
        self.MAX_ACT_EXEC = 1000000
        self.TZ = TZ
        self.DATE_FMT, self.DATE_INI, self.DATE_FIN = DATE_FMT, DATE_INI, None
        self.NUM_TS_MIN_PARA_ACT = 1  # 1 entrada nueva para actualizar
        self.TS_DATA = 600  # Muestreo en segs.(1h)
        self.NUM_RETRIES, self.TIMEOUT, self.MAX_THREADS_REQUESTS = NUM_RETRIES, TIMEOUT, MAX_THREADS_REQUESTS
        self.HEADERS, self.JSON_REQUESTS, self.PARAMS_REQUESTS = None, False, None
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
        # self.printif('\nINFORMACIÓN EN BASE DE DATOS:\n{}'.format(self.store), 'info')

    # you want to override this on the child classes
    def url_data_dia(self, key_dia):
        """Override this method: url = func_url_data_dia(key_dia)"""
        raise NotImplementedError

    # you want to override this on the child classes
    def procesa_data_dia(self, key_dia, response):
        """Override this method: data_import, ok = func_procesa_data_dia(key_dia, response)"""
        raise NotImplementedError

    # you can override this on the child classes
    def post_update_data(self):
        """(Optional) Posibilidad de procesar todos los datos al final del update."""
        pass

    def __get_data_en_intervalo(self, d0=None, df=None):
        """
        Obtiene los datos en bruto de la red realizando múltiples requests al tiempo
        Procesa los datos en bruto obtenidos de la red convirtiendo a Pandas DataFrame
        """

        params = {'date_fmt': self.DATE_FMT,
                  'usar_multithread': self.USAR_MULTITHREAD, 'max_threads_requests': self.MAX_THREADS_REQUESTS,
                  'timeout': self.TIMEOUT, 'num_retries': self.NUM_RETRIES,
                  'func_procesa_data_dia': self.procesa_data_dia, 'func_url_data_dia': self.url_data_dia,
                  'max_act_exec': self.MAX_ACT_EXEC,
                  'data_extra_request': {'headers': self.HEADERS,
                                         'json_req': self.JSON_REQUESTS,
                                         'params_request': self.PARAMS_REQUESTS},
                  'verbose': self.verbose}
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

        :param data_revisar: (OPC) Se puede pasar un dataframe específico
        :param key_revisar: (OPC) Normalmente, para utilizar 'dem'
        :return: tmax, num_entradas
        """
        key_revisar = key_revisar or self.masterkey
        data_revisar = self.data if data_revisar is None else data_revisar
        # return tmax, num_entradas
        if key_revisar in data_revisar.keys():
            data_rev = data_revisar[key_revisar]
            return data_rev.index[-1].to_pydatetime(), len(data_rev)
        else:
            return pd.Timestamp(dt.datetime.strptime(self.DATE_INI, self.DATE_FMT), tz=self.TZ).to_pydatetime(), 0

    def printif(self, obj_print, tipo_print=None):
        """Color output & logging."""
        if tipo_print is None:
            if self.verbose:
                print(obj_print)
        elif tipo_print == 'secc':
            if self.verbose:
                print_secc(obj_print)
        elif tipo_print == 'ok':
            if self.verbose:
                print_ok(obj_print)
            logging.info(obj_print)
        elif tipo_print == 'info':
            if self.verbose:
                print_info(obj_print)
        elif tipo_print == 'error':
            if self.verbose:
                print_err(obj_print)
            logging.error(obj_print)
        elif tipo_print == 'warning':
            if self.verbose:
                print_warn(obj_print)
            logging.warning(obj_print)
        elif tipo_print == 'bold':
            if self.verbose:
                print_bold(obj_print)
        else:
            if self.verbose:
                print_err(obj_print)
            logging.error(obj_print)
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
            if self.DATE_FIN is not None:
                now = min(now, pd.Timestamp(self.DATE_FIN, tz=self.TZ).to_pydatetime())
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
        """Check/Lectura de base de datos hdf en disco (local)."""
        try:
            if forzar_update:
                self.printif('Se procede a actualizar TODOS los datos (force update ON)', 'info')
                assert()
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
                self.printif('--> NO SE LEE DB_HDF (Exception: {}:{})'.format(type(e).__name__, str(e)), 'warning')
                self.printif('--> Se procede a realizar la captura de TODOS los datos existentes:', 'warning')
            data_ant, tmax = None, None
        # Actualización de la base de datos
        data_act, hay_nueva_info = self.__actualiza_datos(data_ant, tmax)
        self.data = data_act
        if hay_nueva_info:  # Grabación de la base de datos hdf en disco (local)
            # Posibilidad de procesar todos los datos al final del update, antes de grabar a disco:
            self.post_update_data()
            self.save_data()
            tmax, num_entradas = self.last_entry()
            self.printif('\tNº rows:\t{} samples\n\tÚltima:\t{:%d-%m-%Y %H:%M}'.format(num_entradas, tmax), 'ok')
        return hay_nueva_info

    def integridad_data(self, data_integr=None, key=None):
        """
        Comprueba que el index de cada dataframe de la base de datos sea de fechas, único (sin duplicados) y creciente
        :param data_integr:
        :param key:
        """
        def _assert_integridad(df):
            if df is not None and not df.empty:
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
        """Show some info."""

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

    def _backup_last_store(self):
        self.store.close()
        bkp_path = os.path.join(os.path.dirname(self.PATH_DATABASE), 'bkp_' + os.path.basename(self.PATH_DATABASE))
        if os.path.exists(bkp_path):
            os.remove(bkp_path)
        os.rename(self.PATH_DATABASE, bkp_path)
        self.store = pd.HDFStore(self.PATH_DATABASE)
        # self.store.put('data', self.data['data'], mode='w')
        # self.store.put('data_dias', self.data['data_dias'], mode='w')
        # self.store.put('errores', self.data['errores'], mode='w')

    def save_data(self, dataframe=None, key_data=None):
        """ Guarda en disco la información
        :param dataframe:
        :param key_data:
        """

        def _save_data_en_key(store, key_save, data_save, func_err):
            try:
                # TODO Revisar errores grabación HDF5 mode table vs fixed:
                # TypeError: unorderable types: NoneType() >= tuple() al grabar HDF5 como 'table'
                # store.put(key_save, data_save, format='table', mode='w')
                store.put(key_save, data_save, mode='w')
            except TypeError as e:
                func_err('ERROR SALVANDO INFO: {}\nKEY: {}; DATA:\n{}'.format(e, key_save, data_save), 'error')

        self.integridad_data()
        self.store.open()
        if dataframe is None:
            if key_data is None:
                # Se elimina (backup) el fichero h5 anterior
                self._backup_last_store()
                for k in self.data.keys():
                    _save_data_en_key(self.store, k, self.data[k], self.printif)
            else:
                _save_data_en_key(self.store, key_data, self.data[key_data], self.printif)
        else:
            _save_data_en_key(self.store, key_data or self.masterkey, dataframe, self.printif)
        self.store.close()

    def load_data(self, key=None, **kwargs):
        """ Lee de disco la información y la devuelve
        :param key:
        """
        self.store.open()
        if key:
            return pd.read_hdf(self.PATH_DATABASE, key, **kwargs)
        else:
            data_load = dict()
            for k in self.store.keys():
                k = k.replace('/', '')
                # **kwargs ej:= where=['index > 2009','index < 2010'],columns=['ordinal']
                data_load[k] = pd.read_hdf(self.PATH_DATABASE, k, **kwargs)
            self.data = data_load
        self.store.close()
        self.integridad_data()

    def append_delta_index(self, ts_data=None, data_delta=None, key=KEY_DATA):
        """Append columns with ∆T between rows to data."""
        reasign = False
        if data_delta is None:
            if self.data is not None:
                data_delta = self.data[key]
            else:
                self.printif('NO HAY DATOS PARA AÑADIR DELTA', 'error')
                return None
            reasign = True
        data_delta['delta'] = data_delta.index.tz_convert('UTC')
        if ts_data is not None:
            data_delta['delta'] = (data_delta['delta'] - data_delta['delta'].shift(1)).fillna(ts_data)
            data_delta['delta_T'] = data_delta['delta'].apply(lambda x: pd.Timedelta(x).seconds) / ts_data
        else:
            data_delta['delta'] = (data_delta['delta'] - data_delta['delta'].shift(1)).fillna(0)
        if reasign:
            self.data[key] = data_delta
        else:
            return data_delta

