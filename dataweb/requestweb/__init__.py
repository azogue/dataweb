# -*- coding: utf-8 -*-
"""
Gestión de datos recogidos en web de forma periódica
@author: Eugenio Panadero
"""
import datetime as dt
from json.decoder import JSONDecodeError
import logging
import numpy as np
import pandas as pd
import requests
from requests.exceptions import ConnectionError, ConnectTimeout, HTTPError
import time
from dataweb.threadingtasks import procesa_tareas_paralelo
from dataweb.mergedataweb import merge_data
from prettyprinting import print_warn, print_err


__author__ = 'Eugenio Panadero'
__copyright__ = "Copyright 2015, AzogueLabs"
__credits__ = ["Eugenio Panadero"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Eugenio Panadero"


# Variables por defecto
NUM_RETRIES = 3
TIMEOUT = 3
USAR_MULTITHREAD = True
MAX_THREADS_REQUESTS = 175
MIN_LEN_REQUEST = 10
MAX_THREADS_MERGE = 500
DIAS_MERGE_MAX = 5
DATE_FMT = '%Y-%m-%d'


def request_data_url(url, headers=None, num_retries=NUM_RETRIES, timeout=TIMEOUT,
                     params_request=None, json_req=False, **kwargs_req):
    """
    Realiza sucesivos intentos de request a una url dada, intentándolo hasta que recibe un status 200 (OK)
    y el texto recibido es mayor que MIN_LEN_REQUEST (=10).
    Resulta útil cuando se están realizando múltiples requests al mismo servidor (por velocidad), y no siempre
    las respuestas que emite son correctas.
    :param url:
    :param headers:
    :param num_retries:
    :param timeout:
    :param params_request:
    :param json_req:
    """
    count, status, response = 0, -1, None
    kwargs_req.update(timeout=timeout)
    while count < num_retries:
        try:
            resp = requests.get(url, headers=headers, params=params_request, **kwargs_req)
            status = resp.status_code
            if status == 200:
                if json_req:
                    response = resp.json()
                else:
                    response = resp.content.decode()
                if json_req or len(response) > MIN_LEN_REQUEST:
                    break  # SALIDA BUCLE WHILE
            elif status == 503:  # HTTP 503 Service unavailable
                print_warn('RECIBIDO ERROR HTTP 503 Service unavailable en url:"{}". Esperando 10 segs'.format(url))
                time.sleep(10)
        except (ConnectionError, ConnectTimeout, HTTPError) as e:
            if count == num_retries - 1:
                print_warn('ERROR: ' + str(e) + ' ¿No hay conexión de internet??')
                print_warn(e.__name__)
                logging.error('ERROR: ' + str(e) + ' ¿No hay conexión de internet??')
            time.sleep(1)
        except JSONDecodeError as e:
            if count == num_retries - 1:
                # print_warn('ERROR: {}\n-> RESPONSE: {}\n{}'.format(e, resp, resp.content))
                # logging.error('ERROR: {}\n-> RESPONSE: {}\n{}'.format(e, resp, resp.content))
                print_warn('ERROR: {}'.format(e))
                logging.error('ERROR: {}'.format(e))
            time.sleep(1)
        except TypeError as e:
            print_warn('TypeError')
            logging.error(str(e))
            print_warn(str(e))
        except Exception as e:
            if count > 0:
                logging.error('%luº Exception no reconocida: %s!!' % (count + 1, type(e)))
                print_err('%luº Exception no reconocida: %s!!' % (count + 1, type(e)))
                print_warn(str(e))
        count += 1
    if count > 0 and count == num_retries:
        print_err('NO SE HA PODIDO OBTENER LA INFO EN %s' % url)
        logging.error('NO SE HA PODIDO OBTENER LA INFO EN %s' % url)
    return status, response


def get_data_en_intervalo(d0=None, df=None, date_fmt=DATE_FMT,
                          usar_multithread=USAR_MULTITHREAD, max_threads_requests=MAX_THREADS_REQUESTS,
                          timeout=TIMEOUT, num_retries=NUM_RETRIES,
                          func_procesa_data_dia=None, func_url_data_dia=None, max_act_exec=None, verbose=True,
                          data_extra_request=None):
        """
        Obtiene los datos en bruto de la red realizando múltiples requests al tiempo
        Procesa los datos en bruto obtenidos de la red convirtiendo a Pandas DataFrame
        """
        def _date(dia_string):
            if dia_string is None:
                return dt.date.today()
            elif type(dia_string) is pd.Timestamp:
                return dia_string.to_datetime().date()
            elif type(dia_string) is not dt.date:
                return dt.datetime.strptime(dia_string, date_fmt).date()
            else:
                return dia_string

        def _procesa_merge_datos_dias(lista_m, dict_data_merge):

            def _merge_datos_dias(key_tarea_merge, dict_merge_dias):
                dict_merge_dias[key_tarea_merge] = merge_data(dict_merge_dias[key_tarea_merge])

            if num_dias > 1 and usar_multithread:
                lista_grupos = list()
                grupos_dias = [lista_m[i:i + DIAS_MERGE_MAX] for i in np.arange(0, num_dias, DIAS_MERGE_MAX)]
                for grupo in grupos_dias:
                    lista_dfs = list()
                    for key_g in grupo:
                        lista_dfs.append(dict_data_merge[key_g])
                    lista_grupos.append(lista_dfs)
                keys_grupos = np.arange(len(lista_grupos))
                dict_merge = dict(zip(keys_grupos, lista_grupos))
                procesa_tareas_paralelo(keys_grupos, dict_merge, _merge_datos_dias,
                                        '\nMERGE DATAFRAMES DE DATOS WEB DIARIOS (%lu GRUPOS)',
                                        usar_multithread, MAX_THREADS_MERGE, verbose=verbose)
                dict_merge_final = {0: [dict_merge[k] for k in dict_merge.keys()]}
                _merge_datos_dias(0, dict_merge_final)
                return dict_merge_final[0]
            else:
                return merge_data(list(dict_data_merge.values()))

        def _hay_errores_en_datos_obtenidos(dict_data_obtenida):
            keys = list(sorted(dict_data_obtenida.keys()))
            data_es_none = [dict_data_obtenida[k] is None for k in keys]
            error = False
            if any(data_es_none):
                df_err = pd.DataFrame({'key': keys, 'is_bad': data_es_none})
                df_err['date'] = df_err['key'].apply(lambda x: pd.Timestamp(x))
                df_err['delta'] = (df_err['date'] - df_err['date'].shift(1)).fillna(3600 * 24)
                df_g = df_err[~df_err['is_bad']].copy()
                df_g['delta_g'] = (df_g['date'] - df_g['date'].shift(1)).fillna(3600 * 24)
                # print(df_err)
                # print(df_err['delta'].describe())
                # print(df_g['delta_g'].describe())

                if df_g['delta_g'].max() < pd.Timedelta(2, 'D'):
                    bad_days = df_err[df_err['is_bad']]['key'].tolist()
                    if verbose:
                        print_err('HAY TAREAS NO REALIZADAS ({}):\n{}'.format(len(bad_days), bad_days))
                    logging.error('HAY TAREAS NO REALIZADAS ({}):\n{}'.format(len(bad_days), bad_days))
                    error = False
                else:
                    if verbose:
                        print_err('NO HAY NINGUNA TAREA REALIZADA!')
                    logging.error('NO HAY NINGUNA TAREA REALIZADA!')
                    bad_days = df_err['key'].tolist()
                    error = True
                for k in bad_days:
                    dict_data_obtenida.pop(k)
            return error

        def _obtiene_request(url, key, headers=None, p_req=None, json_r=False, **kwargs_r):
            if type(url) is list:
                results = [request_data_url(u, headers, num_retries, timeout, p_req, json_r, **kwargs_r) for u in url]
                dict_data[key] = list(zip(*results))
            else:
                stat_response = request_data_url(url, headers, num_retries, timeout, p_req, json_r, **kwargs_r)
                dict_data[key] = stat_response

        def _obtiene_data_dia(key, dict_data_responses):
            url = func_url_data_dia(key)
            extra = dict_data_responses[key] if type(dict_data_responses[key]) is dict else {}
            headers = extra.pop('headers', None)
            json_req = extra.pop('json_req', False)
            params_request = extra.pop('params_request', None)
            try:
                count_process, ok = 0, -1
                while count_process < num_retries and ok != 0:
                    _obtiene_request(url, key, headers, params_request, json_req, **extra)
                    data_import, ok = func_procesa_data_dia(key, dict_data_responses[key][1])
                    if ok == 0:
                        dict_data_responses[key] = data_import
                    elif ok == -2:  # Código de salida temprana:
                        count_process = num_retries
                    count_process += 1
                if ok != 0:
                    dict_data_responses[key] = None
            except Exception as e:
                if verbose:
                    print_err('PROCESANDO DATA!???? (Exception: {}; KEY: {}; URL: {})'.format(e, key, url))
                logging.error('PROCESANDO DATA!???? (Exception: {}; KEY: {}; URL: {})'.format(e, key, url))
                dict_data_responses[key] = None

        tic_ini = time.time()
        lista_dias = [dia.strftime(date_fmt) for dia in pd.date_range(_date(d0), _date(df))]
        if max_act_exec:  # BORRAR. Es para limitar el nº de días adquiridos de golpe.
            lista_dias = lista_dias[:max_act_exec]
        num_dias = len(lista_dias)
        if data_extra_request is None:
            dict_data = dict(zip(lista_dias, np.zeros(num_dias)))
        else:
            dict_data = dict(zip(lista_dias, [data_extra_request.copy() for _ in range(num_dias)]))
        # IMPORTA DATOS Y LOS PROCESA
        procesa_tareas_paralelo(lista_dias, dict_data, _obtiene_data_dia,
                                '\nPROCESADO DE DATOS WEB DE %lu DÍAS',
                                usar_multithread, max_threads_requests, verbose=verbose)
        hay_errores = _hay_errores_en_datos_obtenidos(dict_data)
        # MERGE DATOS
        # print(len(lista_dias), len(dict_data.keys()))
        if not hay_errores and num_dias > 0:
            # data_merge = _procesa_merge_datos_dias(lista_dias, dict_data)
            data_merge = _procesa_merge_datos_dias(list(sorted(dict_data.keys())), dict_data)
            str_resumen_import = '\n%lu días importados [Proceso Total %.2f seg, %.4f seg/día]' \
                                 % (num_dias, time.time() - tic_ini, (time.time() - tic_ini) / float(num_dias))
            return data_merge, hay_errores, str_resumen_import
        else:
            return None, hay_errores, 'ERROR IMPORTANDO!!'


# OLD MODE (vía httplib2)
# def request_data_url(url, http=None, num_retries=NUM_RETRIES, timeout=TIMEOUT):
#     """
#     Realiza sucesivos intentos de request a una url dada, intentándolo hasta que recibe un status 200 (OK)
#     y el texto recibido es mayor que MIN_LEN_REQUEST (=10).
#     Resulta útil cuando se están realizando múltiples requests al mismo servidor (por velocidad), y no siempre
#     las respuestas que emite son correctas.
#     :param url:
#     :param http:
#     :param num_retries:
#     :param timeout:
#     """
#     if http is None:
#         http = Http(timeout=timeout)
#     count, status, response = 0, -1, None
#     while count < num_retries:
#         try:
#             (status, response) = http.request(url)
#             if status['status'] == '200' and len(response) > MIN_LEN_REQUEST:
#                 break  # SALIDA BUCLE WHILE
#         except httplib2.ServerNotFoundError as e:
#             if count == num_retries - 1:
#                 print_warn('ERROR: ' + str(e) + ' ¿No hay conexión de internet??')
#                 logging.error('ERROR: ' + str(e) + ' ¿No hay conexión de internet??')
#         except socket.error as e:  # [Errno 60] Operation timed out
#             if count == num_retries - 2:
#                 print_warn('{}º EXCEPCIÓN: 60 Operation timed out?? [{}] EXC: {}'.format(count + 1, url, e))
#                 logging.debug('{}º EXCEPCIÓN: 60 Operation timed out?? [{}] EXC: {}'.format(count + 1, url, e))
#             time.sleep(1)
#         except (httplib2.HttpLib2Error, httplib2.HttpLib2ErrorWithResponse) as e:  # , urllib.URLError) as e:
#             if count > 1:
#                 print_warn(type(e))
#                 logging.error(type(e))
#                 print_warn('HttpLib2Error?, HttpLib2ErrorWithResponse?, urllib.URLError?')
#                 # notTODO except error: [Errno 60] Operation timed out; ResponseNotReady
#         except TypeError as e:
#             print_warn('TypeError')
#             logging.error(str(e))
#             print_warn(str(e))
#         except Exception as e:
#             if count > 0:
#                 logging.error('%luº Exception no reconocida: %s!!' % (count + 1, type(e)))
#                 print_err('%luº Exception no reconocida: %s!!' % (count + 1, type(e)))
#                 print_warn(str(e))
#         count += 1
#     if count > 0 and count == num_retries:
#         print_err('NO SE HA PODIDO OBTENER LA INFO EN %s' % url)
#         logging.error('NO SE HA PODIDO OBTENER LA INFO EN %s' % url)
#     return status, response
