import pyodbc

# Conectamos con el servidos y la base de datos especificados en el archivo 'atributos.py':

def conect_db(dict):
        server = dict['server']
        database = dict['database']
        specs = ('Driver={SQL Server};'
                f'Server={server};'
                f'Database={database};'
                'Trusted_Connection=yes;')
        con = pyodbc.connect(specs, autocommit=True)
        cursor = con.cursor()
        print('Connection Successfully Established.')
        return con, cursor

# Insertamos los datos a la base de datos:

def insert_data(con, cursor, tables):
    for row in tables['users'].itertuples():
        cursor.execute('''
                    INSERT INTO users (user_id, gender, birth_year, adults_at_home, kids_at_home,
                    pet, province, age_group)
                    VALUES (?,?,?,?,?,?,?,?)
                    ''',
                    row.user_id, 
                    row.gender,
                    row.birth_year,
                    row.adults_at_home,
                    row.kids_at_home,
                    row.pet,
                    row.province,
                    row.age_group
                    )
    print('Data successfully uploaded to "users".')

    for row in tables['tickets'].itertuples():
        cursor.execute('''
                    INSERT INTO tickets (ticket_id, user_id, retailer, payment_method, date,
                    ticket_amount)
                    VALUES (?,?,?,?,?,?)
                    ''',
                    row.ticket_id, 
                    row.user_id,
                    row.retailer,
                    row.payment_method,
                    row.date,
                    row.ticket_amount
                    )
    print('Data successfully uploaded to "tickets".')

    for row in tables['ticket_lines'].itertuples():
        cursor.execute('''
                    INSERT INTO ticket_lines (id, ticket_id, category1_id, category1_name, category2_id, category2_name,
                    product_name, units, total_amount)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ''',
                    row.id, 
                    row.ticket_id,
                    row.category1_id,
                    row.category1_name,
                    row.category2_id,
                    row.category2_name,
                    row.product_name,
                    row.units,
                    row.total_amount
                    )
    print('Data successfully uploaded to "ticket_lines".')

    for row in tables['users_activity'].itertuples():
        cursor.execute('''
                    INSERT INTO users_activity (user_id, total_tickets, preferred_retailer, preferred_payment_method, total_spent)
                    VALUES (?,?,?,?,?)
                    ''',
                    row.user_id, 
                    row.total_tickets,
                    row.preferred_retailer,
                    row.preferred_payment_method,
                    row.total_spent,
                    )
    print('Data successfully uploaded to "users_activity".')
    
    con.commit()

# Desconectamos de la base de datos:

def disconect_db(cursor):
    cursor.close()
    print('Conection closed.')