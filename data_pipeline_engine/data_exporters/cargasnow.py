from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_snowflake(data: dict[str, DataFrame], **kwargs) -> None:
    """
    Exports multiple tables from Instacart's MySQL database to a Snowflake warehouse.
    Ensure your configuration settings are correctly set in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#snowflake
    """
    database = 'INSTACART_DB'  # Reemplaza con el nombre real de tu base en Snowflake
    schema = 'RAW'  # Ajusta según tu configuración en Snowflake
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'  # Asegúrate de que coincide con tu configuración

    # Verifica que los datos sean un diccionario de dataframes
    if not isinstance(data, dict):
        raise ValueError("Expected a dictionary of DataFrames, received:", type(data))

    with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for table_name, df in data.items():
            if not isinstance(df, DataFrame) or df.empty:
                print(f"Skipping {table_name} as it is empty or not a DataFrame")
                continue

            print(f"Exporting {table_name} to Snowflake...")
            loader.export(
                df,
                table_name.upper(),  # Snowflake usa nombres de tabla en mayúsculas
                database,
                schema,
                if_exists='replace',  # Puedes cambiar a 'append' si prefieres no sobrescribir
            )
            print(f"Table {table_name} exported successfully!")

