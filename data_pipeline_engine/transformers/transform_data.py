if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform(data, *args, **kwargs):
    """
    Limpieza y transformación de datos en Mage AI.
    """
    import pandas as pd
    import numpy as np
    
    # Extraer DataFrames desde el diccionario de entrada
    products = data.get("products")
    order_products = data.get("order_products")
    instacart_orders = data.get("instacart_orders")
    aisles = data.get("aisles")
    departments = data.get("departments")
    
    # Verificar que las tablas existen antes de procesarlas
    if any(df is None for df in [products, order_products, instacart_orders, aisles, departments]):
        raise ValueError("Faltan uno o más DataFrames de entrada. Verifica las conexiones upstream en Mage AI.")
    
    # 1. Manejo de Valores Ausentes
    products['product_name'].fillna("missing product", inplace=True)
    instacart_orders['days_since_prior_order'].fillna(0, inplace=True)
    order_products['add_to_cart_order'].fillna(-1, inplace=True)
    
    # 2. Eliminación o Consolidación de Duplicados
    # Unificar productos duplicados con el mismo aisle_id y department_id
    products = products.drop_duplicates(subset=['product_name', 'aisle_id', 'department_id'], keep='first')
    
    # Unificar órdenes completamente idénticas en instacart_orders
    instacart_orders = instacart_orders.drop_duplicates()
    
    # 3. Conversión de Tipos de Datos y Normalización
    order_products['add_to_cart_order'] = order_products['add_to_cart_order'].astype(int)
    instacart_orders['days_since_prior_order'] = instacart_orders['days_since_prior_order'].astype(int)
    
    # Normalizar nombres de productos a minúsculas
    products['product_name'] = products['product_name'].str.lower()
    
    # Eliminar la columna id de instacart_orders
    instacart_orders = instacart_orders.drop(columns=['id'], errors='ignore')
    
    return {
        "products": products,
        "order_products": order_products,
        "instacart_orders": instacart_orders,
        "aisles": aisles,
        "departments": departments
    }

@test
def test_output(output, *args) -> None:
    """
    Verificación del output transformado.
    """
    assert output is not None, 'El output está indefinido'
    assert all(table in output for table in ["products", "order_products", "instacart_orders", "aisles", "departments"]), 'Faltan tablas en el output'
    assert 'product_name' in output["products"].columns, 'Falta la columna product_name en products'
    assert output["instacart_orders"]["days_since_prior_order"].dtype == int, 'El tipo de days_since_prior_order no es int'
    assert output["order_products"]["add_to_cart_order"].dtype == int, 'El tipo de add_to_cart_order no es int'
    assert 'id' not in output["instacart_orders"].columns, 'La columna id no fue eliminada de instacart_orders'
