# Gestión de datos recogidos en web de forma periódica

Clase base para asumir la gestión del respaldo local de una base de datos accesible vía web con actualizaciones diarias.

El histórico de datos desde la fecha de inicio suministrada se gestiona como una o varias Pandas DataFrame's y se guarda en disco en formato HDF5. La base de datos se actualiza hasta el instante actual en cada ejecución, y también proporciona interfaces sencillas para la gestión de dicha base.
Está preparada para datos indexados por tiempo localizado, para gestionar adecuadamente los cambios de hora (DST).

* Requiere únicamente implementar las funciones `func_urls_data_dia` y `func_procesa_data_dia` en la clase hija, junto a los parámetros típicos para el archivo web implementado:
  `path_database`, `DATE_INI`, `DATE_FMT`, `TZ`, `TS_DATA` (periodo de muestreo de las entradas), etc.

  La configuración de los requests a web se realiza mediante `NUM_RETRIES`, `TIMEOUT` y `MAX_THREADS_REQUESTS`. Este último debe tunearse adecuadamente en función del servidor a acceder, con tal de no saturarlo.

## Instalación

```
pip3 install dataweb
```
