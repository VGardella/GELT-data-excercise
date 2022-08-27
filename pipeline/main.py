from funciones import *
from sql_queries import *
from atributos import *

import pandas as pd
import pyodbc
from datetime import datetime
from pathlib import Path

#pd.pipe(data_load(archivos)).pipe(data_cleaning()).pipe(data_analysis()).pipe(data_type_mod())

def data_pipeline(files, group, labels, server_data):
    global users, tickets, ticket_lines, users_activity
    # Importacion y limpieza de los datos:

    data_load(files)
    data_cleaning(files)
    data_analysis(files)
    data_type_mod(files)
    data_organization(files)

    # Exportación a la base de datos de SQL Server:

    conect_db(server_data)
    insert_data()
    disconect_db()

    return 'Every task completed successfully.'

if __name__ == '__main__':
    data_pipeline(archivos, group, labels, server_data)
