#!/usr/bin/env python
import pandas as pd
import numpy as np
from datetime import datetime

#Creamos las tablas

users = pd.read_csv("C:/Users/Pacarena/Documents/GELT_data/users.csv")
tickets = pd.read_csv("C:/Users/Pacarena/Documents/GELT_data/tickets.csv")
ticket_lines = pd.read_csv("C:/Users/Pacarena/Documents/GELT_data/ticket_lines.csv")

# Cambiamos los nombres de alguna columnas:

users.rename(columns = {'id': 'user_id'}, inplace=True)
tickets.rename(columns = {'id': 'ticket_id'}, inplace=True)

#Modificamos los datos:

users['gender'] = users['gender'].replace([0, 1, 2, 3], ['Masculino', 'Femenino', 'No Binario', 'Otro'])
users['kids_at_home'] = users['kids_at_home'].fillna(0)
users['pet'] = users['pet'].fillna('Ninguno')
users['pet'] = users['pet'].replace('0', 'Ninguno')
users['province'] = users['province'].apply(lambda x: x.title())

tickets['retailer'] = tickets['retailer'].apply(lambda x: x.title())
tickets['payment_method'] = tickets['payment_method'].fillna('DES')

# Creamos nuevos atributos:

group = [0, 15, 24, 39, 54, 75, 100]
labels = ['Otros', '15-24', '25-39', '40-54', '55-75', 'Otros']
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

tickets = pd.merge(tickets, ticket_lines[['ticket_id', 'total_amount']].groupby('ticket_id').sum(), \
     'left', left_on='ticket_id', right_on='ticket_id').rename(columns={'total_amount' : 'ticket_amount'})

#Cambiamos el tipo de datos para cada columna para facilitar la inclusion a la base de datos:

users[['gender', 'pet', 'preferred_payment_method']] = users[['gender', 'pet', 'preferred_payment_method']].astype('category')
users[['kids_at_home']] = users[['kids_at_home']].astype('int64')

# Exportamos las tablas:

users.to_csv('C:/Users/Pacarena/Documents/GELT_data/users_limpio.csv', index=False)
tickets.to_csv('C:/Users/Pacarena/Documents/GELT_data/tickets_limpio.csv', index=False)
ticket_lines.to_csv('C:/Users/Pacarena/Documents/GELT_data/ticket_lines_limpio.csv', index=False)

