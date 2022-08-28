import pandas as pd
from datetime import datetime
from pathlib import Path

# Cargamos los archivos .csv:

def data_load(files, tables):
    for x in files:
        data = files[x]['file_name']
        tables[data] = pd.read_csv(files[x]['file_path'])
        print(f'{data} created correctly.')
    return 'All files processed. Use "file_name" as variable name.'

# Hacemos la limpieza de los datos. Los pasos dependeran del nombre de la base de datos a limpiar:

def data_cleaning(files, tables):
    for x in files:
        if files[x]['file_name'] == 'users':

            tables['users'].rename(columns = {'id': 'user_id'}, inplace=True)
            print('"users" column name changed: id -> user_id')

            tables['users']['gender'] = tables['users']['gender'].replace([0, 1, 2, 3], ['Masculino', 'Femenino', 'No Binario', 'Otro'])
            tables['users']['kids_at_home'] = tables['users']['kids_at_home'].fillna(0)
            tables['users']['pet'] = tables['users']['pet'].fillna('Ninguno')
            tables['users']['pet'] = tables['users']['pet'].replace('0', 'Ninguno')
            tables['users']['province'] = tables['users']['province'].apply(lambda x: x.title())
            tables['users']['birth_year'] = tables['users']['birth_year'].fillna(0)

            print('"users" table data corrected.')

        if files[x]['file_name'] == 'tickets':
            
            tables['tickets'].rename(columns = {'id': 'ticket_id'}, inplace=True)
            print('"ticket" column name changed: id -> ticket_id')

            tables['tickets']['retailer'] = tables['tickets']['retailer'].apply(lambda x: x.title())
            tables['tickets']['payment_method'] = tables['tickets']['payment_method'].fillna('DES')
            
            print('"tickets" table data corrected.')

    return 'All tables correctrly cleaned'

# Creamos las nuevas columnas:

def data_analysis(files, tables):
    for x in files:
        if files[x]['file_name'] == 'users':
            group = [0, 15, 24, 39, 54, 75, 100]
            labels = ['Otros', '15-24', '25-39', '40-54', '55-75', 'Otros']
            tables['users']['age_group'] = pd.cut(datetime.today().year - tables['users']['birth_year'], bins = group, labels = labels, ordered=False).fillna('Otros')

            tables['users'] = pd.merge(tables['users'], tables['tickets'][['user_id', 'ticket_id']].groupby('user_id').count(), \
                'left', left_on='user_id', right_on='user_id').rename(columns={'ticket_id': 'total_tickets'})

            tables['users'] = pd.merge(tables['users'], tables['tickets'][['user_id', 'retailer']].groupby('user_id').apply(lambda x: x['retailer'].value_counts().index[0]).rename('preferred_retailer'), \
                'left', left_on='user_id', right_on='user_id')

            tables['users'] = pd.merge(tables['users'], tables['tickets'][['user_id', 'payment_method']].groupby('user_id').apply(lambda x: x['payment_method'].value_counts().index[0]).rename('preferred_payment_method'), \
                'left', left_on='user_id', right_on='user_id')

            total_purchases = pd.DataFrame(pd.merge(tables['tickets'][['user_id', 'ticket_id']], tables['ticket_lines'][['id', 'ticket_id', 'total_amount']], 'left', left_on='ticket_id', right_on='ticket_id'))
            tables['users'] = pd.merge(tables['users'], total_purchases[['user_id', 'total_amount']].groupby('user_id').sum('total_amount'), \
                'right', left_on='user_id', right_on='user_id').rename(columns={'total_amount': 'total_spent'})
            print('New columns added to "users" table.')

        if files[x]['file_name'] == 'tickets':

            tables['tickets'] = pd.merge(tables['tickets'], tables['ticket_lines'][['ticket_id', 'total_amount']].groupby('ticket_id').sum(), \
                'left', left_on='ticket_id', right_on='ticket_id').rename(columns={'total_amount' : 'ticket_amount'})
            print('New columns added to "tickets" table.')
    return 'All new columns created.'

# Modificamos la clase de algunas columnas para facilitar su introduccion a la base de datos:

def data_type_mod(files, tables):
    for x in files:
        if files[x]['file_name'] == 'users':
            tables['users'][['gender', 'pet', 'preferred_payment_method']] = tables['users'][['gender', 'pet', 'preferred_payment_method']].astype('category')
            tables['users'][['kids_at_home']] = tables['users'][['kids_at_home']].astype('int64')
            tables['users']['birth_year'] = tables['users']['birth_year'].astype('int64')
    return 'Data types changed.'

# Creamos una tabla nueva:

def data_organization(files, tables):
    tables['users_activity'] = pd.DataFrame()
    for x in files:
        if files[x]['file_name'] == 'users':
            tables['users_activity'] = tables['users'][['user_id', 'total_tickets', 'preferred_retailer', 'preferred_payment_method', 'total_spent']]
            tables['users'].drop(['total_tickets', 'preferred_retailer', 'preferred_payment_method', 'total_spent'], axis=1, inplace=True)
    files.update({4: {'file_name': 'users_activity', 'file_path': ''}})
    return 'New table "users_activity" created.'

# Funcion opcional de exportación de los datos a archivos .csv:

def data_export(files, tables):
    for x in files:
        if files[x]['file_name'] == 'users':
            tables['users'].to_csv('C:/Users/Pacarena/Documents/GELT_data/users_limpio.csv', index=False)
            print('Exported table "users" to "users_limpio.csv" successfully')
        if files[x]['file_name'] == 'tickets':
            tables['tickets'].to_csv('C:/Users/Pacarena/Documents/GELT_data/tickets_limpio.csv', index=False)
            print('Exported table "tickets" to "tickets_limpio.csv" successfully')
        if files[x]['file_name'] == 'ticket_lines':
            tables['ticket_lines'].to_csv('C:/Users/Pacarena/Documents/GELT_data/ticket_lines_limpio.csv', index=False)
            print('Exported table "ticket_lines" to "ticket_lines_limpio.csv" successfully')
        if files[x]['file_name'] == 'users_activity':
            tables['users_activity'].to_csv('C:/Users/Pacarena/Documents/GELT_data/users_activity_limpio.csv', index=False)
            print('Exported table "users_activity" to "users_activity_limpio.csv" successfully')