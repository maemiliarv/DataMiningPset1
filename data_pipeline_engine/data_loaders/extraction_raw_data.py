from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_snowflake(*args, **kwargs):
    """
    Carga datos desde el esquema RAW en Snowflake.
    """

    # Consultas SQL para extraer las tablas necesarias
    queries = {
        "aisles": "SELECT * FROM RAW.AISLES;",
        "departments": "SELECT * FROM RAW.DEPARTMENTS;",
        "instacart_orders": "SELECT * FROM RAW.INSTACART_ORDERS;",
        "order_products": "SELECT * FROM RAW.ORDER_PRODUCTS;",
        "products": "SELECT * FROM RAW.PRODUCTS;"
        
    }

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Conectar a Snowflake
    with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        data = {table: loader.load(query) for table, query in queries.items()}

    return data  # Retorna un diccionario con DataFrames de cada tabla

@test
def test_output(output, *args) -> None:
    """
    Verifica que las tablas no están vacías.
    """
    assert output is not None, 'La salida está vacía'
    for table_name, df in output.items():
        assert not df.empty, f'La tabla {table_name} no tiene datos'

