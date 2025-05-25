import asyncio
import logging
from typing import Optional, Any, List, Dict, Set, Tuple

import asyncpg
import pandas as pd
from asyncpg import Pool
from pydantic import BaseModel

from org.boxbuilder.database.postgres.models.connection_properties import (
    ConnectionProperties,
)
from org.boxbuilder.database.postgres.models.data_types import DataTypes
from org.boxbuilder.database.postgres.models.table_model import TableModel
from org.boxbuilder.utils.default_utils import coalesce

_LOG = logging.getLogger(__name__)

_POSTGRES_TYPE_TO_ENUM = {
    "TEXT": DataTypes.TEXT,
    "VARCHAR": DataTypes.TEXT,
    "CHAR": DataTypes.TEXT,
    "BOOLEAN": DataTypes.BOOLEAN,
    "NUMERIC": DataTypes.NUMBER,
    "DECIMAL": DataTypes.NUMBER,
    "REAL": DataTypes.NUMBER,
    "DOUBLE PRECISION": DataTypes.NUMBER,
    "INTEGER": DataTypes.NUMBER,
    "BIGINT": DataTypes.NUMBER,
    "SMALLINT": DataTypes.NUMBER,
    "DATE": DataTypes.DATE,
    "TIME": DataTypes.TIME,
    "TIMESTAMP WITHOUT TIME ZONE": DataTypes.TIMESTAMP,
    "TIMESTAMP WITH TIME ZONE": DataTypes.TIMESTAMP_WITH_TIMEZONE,
    "TIME WITHOUT TIME ZONE": DataTypes.TIME,
}


class QueryHelper:

    def __init__(self, connection_properties: ConnectionProperties):
        self._connection_properties: ConnectionProperties = connection_properties
        self._db_pool: Optional[Dict[str, Pool]] = {}

    async def _get_connection_pool(self, database_name: str) -> Pool:
        if database_name not in self._db_pool:
            self._db_pool[database_name] = await asyncpg.create_pool(
                self._connection_properties.build_postgres_connection_url(database_name)
            )
        return self._db_pool[database_name]

    async def create_table(self, table_model: TableModel):
        query = QueryHelper.build_create_table_query(table_model)
        async with (
            await self._get_connection_pool(database_name=table_model.database)
        ).acquire() as connection:
            _LOG.info(f"Going to execute query: {query}")
            await connection.execute(query)

    @staticmethod
    def build_create_table_query(
        table_model: TableModel, replace=False, only_if_not_exists: bool = True
    ) -> str:
        if replace and only_if_not_exists:
            raise RuntimeError(
                "Can't have both replace = True and only_if_not_exists True"
            )

        replace_part = "OR REPLACE" if replace else ""
        if_not_exists_part = "IF NOT EXISTS" if only_if_not_exists else ""
        columns_part = ",".join(
            [
                f'"{c}" {d.value[0]}'
                for c, d in table_model.column_name_to_data_type_map.items()
            ]
        )
        primary_keys = table_model.primary_keys

        primary_keys_part = ""
        if primary_keys is not None and len(primary_keys) > 0:
            primary_keys_str = ", ".join(f'"{pk}"' for pk in primary_keys)
            primary_keys_part = f", PRIMARY KEY ({primary_keys_str})"

        return f"""CREATE {replace_part} TABLE {if_not_exists_part} {table_model.get_fqn()} ({columns_part}{primary_keys_part})"""

    async def insert_dataframe(self, table_model: TableModel, df: pd.DataFrame):
        data_df = QueryHelper._process_df_for_table_insert(table_model, df)
        values = list(data_df.itertuples(index=False, name=None))
        await self._insert_tuples_into_table(table_model, { c: table_model.column_name_to_data_type_map[c] for c in data_df.columns }, values)

    async def insert_pydantic_models(
        self, table_model: TableModel, data_objects: List[BaseModel]
    ):
        final_dicts = QueryHelper._process_pydantic_models_for_insert(
            table_model, data_objects
        )
        columns = sorted(list(table_model.column_name_to_data_type_map.keys()))
        values = [
            tuple(data_dict[column] for column in columns) for data_dict in final_dicts
        ]
        await self._insert_tuples_into_table(table_model, table_model.column_name_to_data_type_map, values)

    async def execute_statement(self, database_name: str, statement: str) -> str:
        pool = await self._get_connection_pool(database_name=database_name)
        async with pool.acquire() as connection:
            async with connection.transaction():
                return await connection.execute(statement)

    async def get_query_results_as_dictionaries(
        self,
        database_name: str,
        query: str,
        params: Optional[List[Any]] = None,
        output_column_name_data_type_mapping: Optional[Dict[str, DataTypes]] = None,
    ) -> Optional[List[Dict[str, Any]]]:

        async with (
            await self._get_connection_pool(database_name=database_name)
        ).acquire() as connection:
            results = await connection.fetch(query, *params if params else [])

        if results is None:
            return None

        processed_results = []
        for row in results:
            row_dict = dict(row)
            processed_row_dict = {}

            if output_column_name_data_type_mapping is None:
                processed_row_dict = row_dict
            else:
                for column_name, value in row_dict.items():
                    if column_name in output_column_name_data_type_mapping:
                        processed_row_dict[column_name] = (
                            output_column_name_data_type_mapping[
                                column_name
                            ].validate_and_convert(value)
                        )
                    else:
                        processed_row_dict[column_name] = value
            processed_results.append(processed_row_dict)
        return processed_results

    async def get_query_results_as_dataframe(
        self,
        database_name: str,
        query: str,
        params: Optional[List[Any]] = None,
        output_column_name_data_type_mapping: Optional[Dict[str, DataTypes]] = None,
    ) -> Optional[pd.DataFrame]:
        async with (
            await self._get_connection_pool(database_name=database_name)
        ).acquire() as connection:
            results = await connection.fetch(query, *params if params else [])

        if not results:
            return None

        rows = [dict(row) for row in results]

        if not output_column_name_data_type_mapping:
            return pd.DataFrame(rows)

        pandas_dtype_mapping = {
            col: dtype.pandas_dtype
            for col, dtype in output_column_name_data_type_mapping.items()
        }
        return pd.DataFrame(rows).astype(pandas_dtype_mapping)

    async def discover_table(
        self, database_name: str, schema_name: str, table_name: str
    ) -> Optional[TableModel]:
        column_name_to_data_type_map, primary_keys = await asyncio.gather(
            self._get_column_name_data_type_mapping(
                database_name, schema_name, table_name
            ),
            self._get_primary_keys(database_name, schema_name, table_name),
        )

        return TableModel(
            database=database_name,
            schema=schema_name,
            table=table_name,
            primary_keys=primary_keys,
            column_name_to_data_type_map=column_name_to_data_type_map,
        )

    async def create_schema(self, database_name: str, schema_name: str):
        async with (
            await self._get_connection_pool(database_name=database_name)
        ).acquire() as connection:
            await connection.execute(
                f"""DO $$ 
    BEGIN 
        CREATE SCHEMA {schema_name};
    EXCEPTION 
        WHEN duplicate_schema THEN 
            NULL;
    END $$
    """
            )

    async def drop_schema(self, database_name: str, schema_name: str):
        async with (
            await self._get_connection_pool(database_name=database_name)
        ).acquire() as connection:
            await connection.execute(
                f"""DROP SCHEMA IF EXISTS {schema_name} CASCADE;"""
            )

    async def is_view(
        self, database_name: str, schema_name: str, table_name: str
    ) -> Optional[bool]:
        is_view_query = """
            SELECT table_type
            FROM information_schema.tables
            WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3;
        """
        results = await self.get_query_results_as_dictionaries(
            database_name=database_name,
            query=is_view_query,
            params=[database_name, schema_name, table_name],
            output_column_name_data_type_mapping={"table_type": DataTypes.TEXT},
        )
        if not results:
            return None
        return results[0]["table_type"] == "VIEW"

    @staticmethod
    def _process_pydantic_models_for_insert(
        table_model: TableModel, data_objects: List[BaseModel]
    ) -> List[Dict[str, Any]]:
        data_dicts = [do.model_dump() for do in data_objects]
        final_dicts = []
        for data_dict in data_dicts:
            final_dict = {}
            QueryHelper._validate_data_and_get_columns_for_insert(
                table_model, data_dict.keys()
            )
            for (
                column_name,
                data_type,
            ) in table_model.column_name_to_data_type_map.items():
                final_dict[column_name] = data_type.validate_and_convert(
                    data_dict.get(column_name)
                )
            final_dicts.append(final_dict)
        return final_dicts

    async def _insert_tuples_into_table(
        self, table_model: TableModel, column_data_type_map: Dict[str, DataTypes], values: List[Tuple[Any, ...]]
    ):
        columns = list(column_data_type_map.keys())
        column_names_part = ", ".join([f'"{c}"' for c in columns])
        placeholders_part = ", ".join([
            QueryHelper._build_placeholder(i, column_data_type_map[c])
            for i, c in enumerate(columns)
        ])
        conflict_part = ", ".join([f'"{pk}"' for pk in table_model.primary_keys])
        update_part = ", ".join(
            [
                f'"{c}" = EXCLUDED."{c}"'
                for c in columns
                if c not in table_model.primary_keys
            ]
        )
        query = f"""
INSERT INTO {table_model.get_fqn()} ({column_names_part}) 
VALUES ({placeholders_part}) 
ON CONFLICT ({conflict_part}) 
DO UPDATE SET {update_part}
"""
        async with (
            await self._get_connection_pool(database_name=table_model.database)
        ).acquire() as connection:
            async with connection.transaction():
                _LOG.info(f"Going to insert data into {table_model.model_dump()} with query: {query}. Values: {values}")
                await connection.executemany(query, values)

    @staticmethod
    def _build_placeholder(index: int, data_type: DataTypes) -> str:
        casting_part = "" if data_type != DataTypes.JSON else "::jsonb"
        return f"${index+1}{casting_part}"

    @staticmethod
    def _process_df_for_table_insert(
        table_model: TableModel, df: pd.DataFrame
    ) -> pd.DataFrame:

        common_columns = QueryHelper._validate_data_and_get_columns_for_insert(
            table_model, df.columns
        )
        data_df = df[[*common_columns]].astype("object")
        data_df = data_df.map(lambda x: None if pd.isna(x) else x)

        for column in common_columns:
            data_type = table_model.column_name_to_data_type_map.get(column)
            if data_type is None:
                raise RuntimeError(f"Unable to identify data type for column {column}.")
            data_df[column] = data_df[column].map(
                lambda v: data_type.validate_and_convert(v)
            )

        return data_df

    @staticmethod
    def _validate_data_and_get_columns_for_insert(
        table_model: TableModel, columns: List[str]
    ):
        common_columns = set(columns).intersection(
            table_model.column_name_to_data_type_map.keys()
        )
        if len(common_columns) == 0:
            raise RuntimeError(f"No columns common between Data and table.")
        if table_model.primary_keys is not None:
            primary_key_columns_present = common_columns.intersection(
                set(table_model.primary_keys)
            )
            primary_key_columns_absent = set(table_model.primary_keys).difference(
                primary_key_columns_present
            )
            if len(primary_key_columns_absent) > 0:
                raise RuntimeError(
                    f"Data is missing primary key columns: {primary_key_columns_absent}"
                )
        return common_columns

    async def _get_column_name_data_type_mapping(
        self, database_name: str, schema_name: str, table_name: str
    ) -> Dict[str, DataTypes]:
        column_query = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3;
        """
        results = await self.get_query_results_as_dictionaries(
            database_name=database_name,
            query=column_query,
            params=[database_name, schema_name, table_name],
            output_column_name_data_type_mapping={
                "column_name": DataTypes.TEXT,
                "data_type": DataTypes.TEXT,
            },
        )
        column_name_to_data_type_map = {}
        for row in results:
            column_name = row["column_name"]
            postgres_data_type = row["data_type"].upper()
            mapped_data_type = _POSTGRES_TYPE_TO_ENUM.get(postgres_data_type)

            if mapped_data_type is not None:
                column_name_to_data_type_map[column_name] = mapped_data_type
            else:
                _LOG.warning(
                    f"Unrecognized PostgreSQL data type: {postgres_data_type} for column {column_name}"
                )
        return column_name_to_data_type_map

    async def _get_primary_keys(
        self, database_name: str, schema_name: str, table_name: str
    ) -> Optional[List[str]]:
        primary_keys_query = """
                    SELECT kc.column_name
                    FROM information_schema.key_column_usage kc
                    JOIN information_schema.table_constraints tc 
                        ON kc.table_catalog = tc.table_catalog
                        AND kc.table_schema = tc.table_schema
                        AND kc.table_name = tc.table_name
                        AND kc.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                        AND kc.table_catalog = $1
                        AND kc.table_schema = $2
                        AND kc.table_name = $3
                    ORDER BY kc.column_name ASC
                """
        results = await self.get_query_results_as_dictionaries(
            database_name=database_name,
            query=primary_keys_query,
            params=[database_name, schema_name, table_name],
            output_column_name_data_type_mapping={"column_name": DataTypes.TEXT},
        )

        pkeys = [v for v in {row["column_name"] for row in coalesce(results, set())}]

        if len(pkeys) == 0:
            return None
        else:
            return pkeys

    async def close(self):
        for pool in self._db_pool.values():
            await pool.close()
        self._db_pool.clear()
