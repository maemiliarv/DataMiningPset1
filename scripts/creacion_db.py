import pandas as pd
import numpy as np
import mysql.connector
import os

# Configuración de conexión a MySQL
conexion = mysql.connector.connect(
    host="localhost",
    user="root",
    password="ServidorMySQL",  
)

cursor = conexion.cursor()

# Crear la base de datos si no existe
cursor.execute("CREATE DATABASE IF NOT EXISTS instacart_db;")
cursor.execute("USE instacart_db;")

# Crear la tabla 'aisles' si no existe
cursor.execute("""
    CREATE TABLE IF NOT EXISTS aisles (
        aisle_id INT PRIMARY KEY,
        aisle VARCHAR(255)
    );
""")

# Crear la tabla 'departments' si no existe
cursor.execute("""
    CREATE TABLE IF NOT EXISTS departments (
        department_id INT PRIMARY KEY,
        department VARCHAR(255)
    );
""")

# Crear la tabla 'products' si no existe
cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INT PRIMARY KEY,
        product_name VARCHAR(255),
        aisle_id INT,
        department_id INT,
        FOREIGN KEY (aisle_id) REFERENCES aisles(aisle_id),
        FOREIGN KEY (department_id) REFERENCES departments(department_id)
    );
""")

# Crear la tabla 'instacart_orders' si no existe
cursor.execute("""
    CREATE TABLE IF NOT EXISTS instacart_orders (
        id INT AUTO_INCREMENT PRIMARY KEY,
        order_id INT,
        user_id INT,
        order_number INT,
        order_dow INT,
        order_hour_of_day INT,
        days_since_prior_order FLOAT 
    );
""")

# Crear la tabla 'order_products' si no existe
cursor.execute("""
    CREATE TABLE IF NOT EXISTS order_products (
        order_id INT,
        product_id INT,
        add_to_cart_order FLOAT,
        reordered INT,
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );
""")

print("✅ Tablas creadas o verificadas correctamente.")

# Obtener la ruta absoluta del script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Rutas de los archivos CSV (ajústalas según tu estructura de carpetas)
aisles_csv = os.path.join(script_dir, "..", "data", "aisles.csv")
departments_csv = os.path.join(script_dir, "..", "data", "departments.csv")
products_csv = os.path.join(script_dir, "..", "data", "products.csv")
orders_csv = os.path.join(script_dir, "..", "data", "instacart_orders.csv")
order_products_csv = os.path.join(script_dir, "..", "data", "order_products.csv")

# Leer los archivos CSV tal como están
df_aisles = pd.read_csv(aisles_csv, sep=";")
df_departments = pd.read_csv(departments_csv, sep=";")
df_products = pd.read_csv(products_csv, sep=";")
df_orders = pd.read_csv(orders_csv, sep=";")
df_order_products = pd.read_csv(order_products_csv, sep=";")

# Convertir NaN en None para que MySQL lo almacene como NULL
df_products = df_products.where(pd.notnull(df_products), None)
df_orders = df_orders.assign(days_since_prior_order=df_orders['days_since_prior_order'].replace({np.nan: None}))
df_order_products = df_order_products.assign(add_to_cart_order=df_order_products['add_to_cart_order'].replace({np.nan: None}))

# Insertar los datos en la tabla 'aisles'
query_aisles = "INSERT INTO aisles (aisle_id, aisle) VALUES (%s, %s)"
for _, row in df_aisles.iterrows():
    cursor.execute(query_aisles, (row['aisle_id'], row['aisle']))

# Insertar los datos en la tabla 'departments'
query_departments = "INSERT INTO departments (department_id, department) VALUES (%s, %s)"
for _, row in df_departments.iterrows():
    cursor.execute(query_departments, (row['department_id'], row['department']))

# Insertar los datos en la tabla 'products'
query_products = "INSERT INTO products (product_id, product_name, aisle_id, department_id) VALUES (%s, %s, %s, %s)"
for _, row in df_products.iterrows():
    cursor.execute(query_products, (row['product_id'], row['product_name'], row['aisle_id'], row['department_id']))

# Insertar los datos en la tabla 'instacart_orders'
query_orders = "INSERT INTO instacart_orders (order_id, user_id, order_number, order_dow, order_hour_of_day, days_since_prior_order) VALUES (%s, %s, %s, %s, %s, %s)"
for _, row in df_orders.iterrows():
    cursor.execute(query_orders, (row['order_id'], row['user_id'], row['order_number'], row['order_dow'], row['order_hour_of_day'], row['days_since_prior_order']))

# Insertar los datos en la tabla 'order_products'
query_order_products = "INSERT INTO order_products (order_id, product_id, add_to_cart_order, reordered) VALUES (%s, %s, %s, %s)"
for _, row in df_order_products.iterrows():
    cursor.execute(query_order_products, (row['order_id'], row['product_id'], row['add_to_cart_order'], row['reordered']))

# Confirmar los cambios
conexion.commit()

print("✅ Datos insertados correctamente en todas las tablas.")

# Cerrar la conexión
cursor.close()
conexion.close()




