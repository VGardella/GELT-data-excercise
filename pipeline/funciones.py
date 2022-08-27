import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

def data_load(files):
    for x in files:
        data = files[x]['file_name']
        globals()[data] = pd.read_csv(files[x]['file_path'])
        print(f'{data} created correctly.')
    return 'All files processed. Use "file_name" as variable name.'

def data_cleaning(files):
    global users, tickets, ticket_lines
    for x in files:
        if files[x]['file_name'] == 'users':

            users.rename(columns = {'id': 'user_id'}, inplace=True)
            print('"users" column name changed: id -> user_id')

            users['gender'] = users['gender'].replace([0, 1, 2, 3], ['Masculino', 'Femenino', 'No Binario', 'Otro'])
            users['kids_at_home'] = users['kids_at_home'].fillna(0)
            users['pet'] = users['pet'].fillna('Ninguno')
            users['pet'] = users['pet'].replace('0', 'Ninguno')
            users['province'] = users['province'].apply(lambda x: x.title())
            print('"users" table data corrected.')

        if files[x]['file_name'] == 'tickets':
            
            tickets.rename(columns = {'id': 'ticket_id'}, inplace=True)
            print('"ticket" column name changed: id -> ticket_id')

            tickets['retailer'] = tickets['retailer'].apply(lambda x: x.title())
            tickets['payment_method'] = tickets['payment_method'].fillna('DES')
            print('"tickets" table data corrected.')

    return 'All tables correctrly cleaned'

def data_analysis(files):
    global labels, groups, users, tickets, ticket_lines
    for x in files:
        if files[x]['file_name'] == 'users':

            users['age_group'] = pd.cut(datetime.today().year - users['birth_year'], bins = group, labels = labels, ordered=False).fillna('Otros')

            users = pd.merge(users, tickets[['user_id', 'ticket_id']].groupby('user_id').count(), \
                'left', left_on='user_id', right_on='user_id').rename(columns={'ticket_id': 'total_tickets'})

            users = pd.merge(users, tickets[['user_id', 'retailer']].groupby('user_id').apply(lambda x: x['retailer'].value_counts().index[0]).rename('preferred_retailer'), \
                'left', left_on='user_id', right_on='user_id')

            users = pd.merge(users, tickets[['user_id', 'payment_method']].groupby('user_id').apply(lambda x: x['payment_method'].value_counts().index[0]).rename('preferred_payment_method'), \
                'left', left_on='user_id', right_on='user_id')

            total_purchases = pd.DataFrame(pd.merge(tickets[['user_id', 'ticket_id']], ticket_lines[['id', 'ticket_id', 'total_amount']], 'left', left_on='ticket_id', right_on='ticket_id'))
            users = pd.merge(users, total_purchases[['user_id', 'total_amount']].groupby('user_id').sum('total_amount'), \
                'right', left_on='user_id', right_on='user_id').rename(columns={'total_amount': 'total_spent'})
            print('New columns added to "users" table.')

        if files[x]['file_name'] == 'tickets':

            tickets = pd.merge(tickets, ticket_lines[['ticket_id', 'total_amount']].groupby('ticket_id').sum(), \
                'left', left_on='ticket_id', right_on='ticket_id').rename(columns={'total_amount' : 'ticket_amount'})
            print('New columns added to "tickets" table.')
    return 'All new columns created.'

def data_type_mod(files):
    for x in files:
        if files[x]['file_name'] == 'users':
            users[['gender', 'pet', 'preferred_payment_method']] = users[['gender', 'pet', 'preferred_payment_method']].astype('category')
            users[['kids_at_home']] = users[['kids_at_home']].astype('int64')
    return 'Data types changed.'

def data_organization(files):
    users_activity = pd.DataFrame()
    for x in files:
        if files[x]['file_name'] == 'users':
            users_activity = users[['user_id', 'total_tickets', 'preferred_retailer', 'preferred_payment_method', 'total_spent']]
            users.drop(['total_tickets', 'preferred_retailer', 'preferred_payment_method', 'total_spent'], axis=1, inplace=True)
    files.update({4: {'file_name': 'users_activity', 'file_path': ''}})
    return 'New table "users_activity" created.'

def data_export(files):
    for x in files:
        if files[x]['file_name'] == 'users':
            users.to_csv('C:/Users/Pacarena/Documents/GELT_data/users_limpio.csv', index=False)
        if files[x]['file_name'] == 'tickets':
            tickets.to_csv('C:/Users/Pacarena/Documents/GELT_data/tickets_limpio.csv', index=False)
        if files[x]['file_name'] == 'ticket_lines':
            ticket_lines.to_csv('C:/Users/Pacarena/Documents/GELT_data/ticket_lines_limpio.csv', index=False)
        if files[x]['file_name'] == 'users_activity':
            ticket_lines.to_csv('C:/Users/Pacarena/Documents/GELT_data/users_activity_limpio.csv', index=False)