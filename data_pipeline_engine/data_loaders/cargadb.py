from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_mysql(*args, **kwargs):
    """
    Loads all tables from the 'instacart_db' MySQL database.
    Ensure your configuration is correctly set in 'io_config.yaml'.
    
    This function retrieves all tables from the database and returns 
    a dictionary where each key is a table name and the value is a dataframe.
    
    Docs: https://docs.mage.ai/design/data-loading#mysql
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'  # Asegúrate de que coincide con tu configuración

    tables = ['aisles', 'departments', 'products', 'instacart_orders', 'order_products']  # Lista de tablas en instacart_db
    data = {}

    # Cargar la configuración y establecer la conexión con MySQL
    with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for table in tables:
            query = f"SELECT * FROM {table};"
            data[table] = loader.load(query)  # Carga la tabla y la almacena en el diccionario

    return data  # Devuelve un diccionario con todas las tablas como DataFrames


@test
def test_output(output, *args) -> None:
    """
    Verifies that all tables have been correctly loaded from MySQL.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, dict), 'Output should be a dictionary'
    
    required_tables = {'aisles', 'departments', 'products', 'instacart_orders', 'order_products'}
    assert set(output.keys()) == required_tables, f'Missing tables: {required_tables - set(output.keys())}'

    for table, df in output.items():
        assert not df.empty, f'Table {table} is empty'
