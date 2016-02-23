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

import threading
import time

import numpy as np
# Colored output
from prettyprinting import print_ok, print_bold


def procesa_tareas_paralelo(lista_tareas, dict_data, func_process,
                            titulo=None, usar_multithread=True, max_threads=100):
    """
    Procesa las tareas diarias en paralelo, limitando a un MAX de nº de threads. Especialmente útil para realizar
    requests simultáneos a la web. Recibe una lista de tareas únicas (key_tarea) a realizar, junto con un diccionario
    de la forma '{key_tarea : variable_in_out}', de forma que cada hilo de ejecución opera con su clave de tarea,
    tomando los datos del diccionario y depositando su salida en el mismo lugar.
    * Necesita el puntero a función 'func_process', cuya definición debe ser de la forma:
        'func_process(key_tarea, dict_data_in_out)'
    """

    num_tareas = len(lista_tareas)
    if titulo and num_tareas > 1:
        print_bold(titulo % num_tareas)
    if num_tareas > 1 and usar_multithread:
        tic_init = time.time()
        threads = [threading.Thread(target=func_process, args=(tarea, dict_data,)) for tarea in lista_tareas]
        lista_threads = [threads[i:i + max_threads] for i in np.arange(0, len(threads), max_threads)]
        cont_tareas = 0
        for th in lista_threads:
            tic = time.time()
            [thread.start() for thread in th]
            [thread.join() for thread in th]
            print(u"Procesado de tareas en paralelo [%lu->%lu, %%=%.1f]: %.2f seg [%.4f seg/tarea]"
                  % (cont_tareas + 1, cont_tareas + len(th), 100. * (cont_tareas + len(th)) / float(num_tareas),
                     (time.time() - tic), (time.time() - tic) / len(th)))
            cont_tareas += len(th)
        tic_fin = (time.time() - tic_init)
        if num_tareas > 1 and usar_multithread and len(lista_threads) > 1:
            print_ok(u"Tiempo de proceso de tareas en paralelo TOTAL (%lu tareas): %.2f seg [%.4f seg/tarea]"
                     % (num_tareas, tic_fin, tic_fin / num_tareas))
    else:
        for tarea in lista_tareas:
            if num_tareas > 3:
                print_bold('Tarea: %s' % str(tarea))
            func_process(tarea, dict_data)
