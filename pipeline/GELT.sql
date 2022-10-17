CREATE DATABASE Gelt
USE Gelt;
USE master
DROP DATABASE Gelt

-- Se crean las tablas iniciales.

CREATE TABLE dbo.users (
	user_id INT NOT NULL PRIMARY KEY,
	gender NVARCHAR(50) NULL,
	birth_year NVARCHAR(10) NULL,
	adults_at_home TINYINT NOT NULL,
	kids_at_home FLOAT NOT NULL,
	pet NVARCHAR(50) NOT NULL,
	province NVARCHAR(50) NOT NULL,
	age_group NVARCHAR(50) NOT NULL,
	total_tickets SMALLINT NULL,
	preferred_retailer NVARCHAR(100) NULL,
	preferred_payment_method NVARCHAR(10) NOT NULL,
	total_spent FLOAT NULL
	)
BULK INSERT dbo.users
	FROM 'C:\Users\Pacarena\Documents\GELT_data\users_limpio.csv'
	WITH (FORMAT='CSV',
	FIRSTROW = 2,
	KEEPNULLS,
	FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '0x0a')

CREATE TABLE dbo.tickets (
	ticket_id INT NOT NULL PRIMARY KEY,
	user_id INT NOT NULL,
	retailer NVARCHAR(100) NULL,
	payment_method NVARCHAR(25) NOT NULL,
	date DATETIME NOT NULL,
	ticket_amount FLOAT NOT NULL
	)
BULK INSERT dbo.tickets
	FROM 'C:\Users\Pacarena\Documents\GELT_data\tickets_limpio.csv'
	WITH (FORMAT='CSV',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '0x0A')

CREATE TABLE dbo.ticket_lines (
	id INT NOT NULL PRIMARY KEY,
	ticket_id INT NOT NULL,
	category1_id NVARCHAR(50) NULL,
	category1_name NVARCHAR(50) NULL,
	category2_id NVARCHAR(50) NULL,
	category2_name NVARCHAR(50) NULL,
	product_name NVARCHAR(MAX) NOT NULL,
	units FLOAT NOT NULL,
	total_amount FLOAT NOT NULL
	)
BULK INSERT dbo.ticket_lines
	FROM 'C:\Users\Pacarena\Documents\GELT_data\ticket_lines_limpio.csv'
	WITH (FORMAT='CSV',
	FIRSTROW = 2,
	FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '0x0A')

-- Se separa la tabla users en dos tablas diferentes, users y users_activity.

SELECT user_id, total_tickets, preferred_retailer, preferred_payment_method, total_spent INTO users_activity FROM users;
ALTER TABLE users DROP COLUMN total_tickets, preferred_retailer, preferred_payment_method, total_spent;

-- Agregamos claves.

ALTER TABLE users_activity ADD CONSTRAINT PK_user_id PRIMARY KEY (user_id);
ALTER TABLE users_activity ADD CONSTRAINT FK_users_id_activity FOREIGN KEY (user_id) REFERENCES users(user_id);

ALTER TABLE tickets ADD CONSTRAINT FK_users_id FOREIGN KEY (user_id) REFERENCES users(user_id);

ALTER TABLE ticket_lines ADD CONSTRAINT FK_ticket_id FOREIGN KEY (ticket_id) REFERENCES tickets(ticket_id);

-- Queries de interes:

-- Ranking de supermercados:
SELECT retailer, COUNT(user_id) AS numero_de_clientes 
FROM tickets GROUP BY retailer ORDER BY numero_de_clientes DESC;

-- Canasta básica (segun categoria general y específica):

SELECT category1_name, COUNT(id) AS numero_de_veces 
FROM ticket_lines GROUP BY category1_name ORDER BY numero_de_veces DESC;

SELECT category1_name, category2_name, COUNT(id) AS numero_de_veces 
FROM ticket_lines GROUP BY category1_name, category2_name ORDER BY numero_de_veces DESC;

-- Medio de pago favorito por rango de edad:

SELECT age_group, MAX(preferred_payment_method) AS preferred_method 
FROM users, users_activity GROUP BY age_group;