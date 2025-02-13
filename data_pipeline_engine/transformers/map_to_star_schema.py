if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform_to_star_schema(data, *args, **kwargs):
    """
    Transforma los datos limpios en un esquema en estrella.
    """
    import pandas as pd
    
    # Extraer DataFrames desde el diccionario de entrada
    products = data.get("products")
    order_products = data.get("order_products")
    instacart_orders = data.get("instacart_orders")
    aisles = data.get("aisles")
    departments = data.get("departments")
    
    # Unir productos con aisles y departments
    dim_products = (products
                    .merge(aisles, on='aisle_id', how='left')
                    .merge(departments, on='department_id', how='left')
                   )
    
    # Renombrar columnas según la estructura deseada
    dim_products = dim_products.rename(columns={
        'id': 'product_id',
        'name_x': 'product_name',
        'name_y': 'aisle',
        'name': 'department'
    })
    
    # Seleccionar solo las columnas necesarias
    dim_products = dim_products[['product_id', 'product_name', 'aisle_id', 'aisle', 'department_id', 'department']]
    
    # Crear tabla de hechos combinada
    fact_orders_products = (order_products
                             .merge(instacart_orders, on='order_id', how='left')
                            )
    
    # Seleccionar y renombrar columnas según la estructura deseada
    fact_orders_products = fact_orders_products[['order_id', 'user_id', 'order_number', 'order_dow', 
                                                 'order_hour_of_day', 'days_since_prior_order', 'product_id', 
                                                 'add_to_cart_order', 'reordered']]
    
    return {
        "DIM_PRODUCTS": dim_products,
        "FACT_ORDERS_PRODUCTS": fact_orders_products
    }

@test
def test_output(output, *args) -> None:
    """
    Verifica que las tablas del esquema en estrella se generaron correctamente.
    """
    assert output is not None, 'El output está indefinido'
    assert 'DIM_PRODUCTS' in output, 'Falta la tabla DIM_PRODUCTS'
    assert 'FACT_ORDERS_PRODUCTS' in output, 'Falta la tabla FACT_ORDERS_PRODUCTS'
    assert not output['DIM_PRODUCTS'].empty, 'La tabla DIM_PRODUCTS está vacía'
    assert not output['FACT_ORDERS_PRODUCTS'].empty, 'La tabla FACT_ORDERS_PRODUCTS está vacía'
    assert 'product_id' in output['DIM_PRODUCTS'].columns, 'Falta product_id en DIM_PRODUCTS'
    assert 'order_id' in output['FACT_ORDERS_PRODUCTS'].columns, 'Falta order_id en FACT_ORDERS_PRODUCTS'
