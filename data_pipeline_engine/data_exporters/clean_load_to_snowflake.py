from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_snowflake(data: dict, **kwargs) -> None:
    """
    Exporta las tablas del esquema en estrella a Snowflake en el esquema CLEAN.
    """

    # Configuración de conexión a Snowflake
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    database = "INSTACART_DB"
    schema = "CLEAN"

    # Crear conexión con Snowflake
    with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        
        # Cambiar al esquema CLEAN en tiempo de ejecución
        loader.execute(f"USE SCHEMA {schema};")

        # Exportar las tablas generadas en el transformador
        tables = {
            "DIM_PRODUCTS": data["DIM_PRODUCTS"],
            "FACT_ORDERS_PRODUCTS": data["FACT_ORDERS_PRODUCTS"]
        }

        for table_name, df in tables.items():
            print(f"Cargando {table_name} en Snowflake...")
            loader.export(
                df,
                table_name,
                database,
                schema,
                if_exists='replace'  # Reemplaza la tabla si ya existe
            )

        print("✅ Todas las tablas fueron cargadas exitosamente en Snowflake.")



