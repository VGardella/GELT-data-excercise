from funciones import *
from sql_queries import *
from atributos import *

import pandas as pd
import pyodbc
from datetime import datetime
from pathlib import Path

#pd.pipe(data_load(archivos)).pipe(data_cleaning()).pipe(data_analysis()).pipe(data_type_mod())
tablas = {}

def data_pipeline(files, tables, server_data):

    # Importacion y limpieza de los datos:

    data_load(files, tables)
    data_cleaning(files, tables)
    data_analysis(files, tables)
    data_type_mod(files, tables)
    data_organization(files, tables)

    # Exportación a la base de datos de SQL Server:

    con, cursor = conect_db(server_data)
    insert_data(con, cursor, tables)
    disconect_db(cursor)

    return 'Every task completed successfully.'

if __name__ == '__main__':
    data_pipeline(archivos, tablas, server_data)
