# Diccionario con los archivos a procesar:

archivos = {1: {'file_name': 'users', 'file_path': 'C:/Users/Pacarena/Documents/GELT_data/users.csv'}, 
2: {'file_name': 'tickets', 'file_path':'C:/Users/Pacarena/Documents/GELT_data/tickets.csv'}, 
3: {'file_name': 'ticket_lines', 'file_path': 'C:/Users/Pacarena/Documents/GELT_data/ticket_lines.csv'}}

# Decfinimos los parámetros para crear la columna 

group = [0, 15, 24, 39, 54, 75, 100]
labels = ['Otros', '15-24', '25-39', '40-54', '55-75', 'Otros']

server_data = {'server': 'CUCALAGRANDE\SQLEXPRESS', 'database':'Gelt'}