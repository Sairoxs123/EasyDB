import os
import csv
import json
from cs50 import SQL
from typing import Any, Dict, List, Optional, Union
from contextlib import contextmanager
import sqlite3
from sqlite3 import Error as SQLiteError

class DatabaseError(Exception):
    """Base exception for database errors"""
    pass

class ValidationError(Exception):
    """Exception raised for validation errors"""
    pass

class QuerySet:
    def __init__(self, data: Dict[str, Any], dbpath: str, table: str):
        self.data = data
        self.db = SQL(f"sqlite:///{dbpath}")
        self.table = table

    def update(self, **kwargs) -> None:
        if not kwargs:
            raise ValidationError("Fields to be updated must be passed as parameter in update function.")

        try:
            set_clauses = []
            for key, value in kwargs.items():
                if not isinstance(key, str):
                    raise ValidationError("Column names must be strings")
                if value is None:
                    set_clauses.append(f"{key} = NULL")
                elif isinstance(value, (int, float)):
                    set_clauses.append(f"{key} = {value}")
                else:
                    set_clauses.append(f"{key} = '{self._escape_string(str(value))}'")
            set_clause_str = ", ".join(set_clauses)

            where_clauses = []
            for key, value in self.data.items():
                if value is None:
                    where_clauses.append(f"{key} IS NULL")
                elif isinstance(value, (int, float)):
                    where_clauses.append(f"{key} = {value}")
                else:
                    where_clauses.append(f"{key} = '{self._escape_string(str(value))}'")
            where_clause_str = " AND ".join(where_clauses)

            sql = f"UPDATE {self.table} SET {set_clause_str} WHERE {where_clause_str}"
            self.db.execute(sql)
        except SQLiteError as e:
            raise DatabaseError(f"Error updating record: {str(e)}")

    def delete(self) -> None:
        try:
            where_clauses = []
            for key, value in self.data.items():
                if value is None:
                    where_clauses.append(f"{key} IS NULL")
                elif isinstance(value, (int, float)):
                    where_clauses.append(f"{key} = {value}")
                else:
                    where_clauses.append(f"{key} = '{self._escape_string(str(value))}'")
            where_clause_str = " AND ".join(where_clauses)

            sql = f"DELETE FROM {self.table} WHERE {where_clause_str}"
            self.db.execute(sql)
        except SQLiteError as e:
            raise DatabaseError(f"Error deleting record: {str(e)}")

    def _escape_string(self, s: str) -> str:
        """Escape special characters in string values"""
        return s.replace("'", "''")

class Model:
    def __init__(self, name: str):
        if not isinstance(name, str):
            raise ValidationError("Table name must be a string")
        if not name.isidentifier():
            raise ValidationError("Invalid table name")

        self.sql, self.path = self._initialize_db(name)
        self.name = name
        self.db = SQL(f"sqlite:///{self.path}")
        self._connection_pool = []
        self._max_connections = 5

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        if len(self._connection_pool) < self._max_connections:
            conn = sqlite3.connect(self.path)
            self._connection_pool.append(conn)
        else:
            conn = self._connection_pool.pop(0)

        try:
            yield conn
        finally:
            if conn in self._connection_pool:
                self._connection_pool.remove(conn)
            conn.close()

    def _initialize_db(self, name: str) -> tuple[str, str]:
        try:
            db_path = os.path.join(os.getcwd(), "db.sqlite3")
            if not os.path.exists(db_path):
                with open(db_path, "w") as f:
                    pass
            sql = f"CREATE TABLE `{name}` ()"
            return sql, db_path
        except Exception as e:
            raise DatabaseError(f"Error initializing database: {str(e)}")

    def addColumn(self, name: str, type: str, **kwargs) -> None:
        if not isinstance(name, str):
            raise ValidationError("Column name must be a string")
        if not name.isidentifier():
            raise ValidationError("Invalid column name")
        if type not in ["int", "str", "date"]:
            raise ValidationError("Invalid column type")

        try:
            sql = f"`{name}` "
            if type == "int":
                primary = kwargs.get("primary_key", False)
                if primary:
                    sql += "INTEGER PRIMARY KEY AUTOINCREMENT"
                else:
                    null_arg = kwargs.get("null", False)
                    null = "NOT NULL" if not null_arg else ""
                    sql += f"int(11) {null}"
            elif type == "str":
                max_length = kwargs.get("max_length", 256)
                if not isinstance(max_length, int) or max_length <= 0:
                    raise ValidationError("max_length must be a positive integer")
                null_arg = kwargs.get("null", False)
                null = "NOT NULL" if not null_arg else ""
                sql += f"varchar({max_length}) {null}"
            elif type == "date":
                null_arg = kwargs.get("null", False)
                null = "NOT NULL" if not null_arg else ""
                sql += f"DATE {null}"

            self.sql = self.sql[:-1]
            if self.sql[-1] != "(":
                self.sql += ", "
            self.sql += sql + ")"
        except Exception as e:
            raise DatabaseError(f"Error adding column: {str(e)}")

    def removeColumn(self, name):
        self.db.execute(f"ALTER TABLE {self.name} DROP COLUMN {name}")

    def renameColumn(self, old, new):
        self.db.execute(f"ALTER TABLE {self.name} RENAME COLUMN {old} TO {new}")

    def create(self) -> None:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.sql)
                conn.commit()
        except SQLiteError as e:
            raise DatabaseError(f"Error creating table: {str(e)}")

    def drop(self) -> None:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {self.name}")
                conn.commit()
        except SQLiteError as e:
            raise DatabaseError(f"Error dropping table: {str(e)}")

    def all(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[QuerySet]:
        try:
            sql = f"SELECT * FROM {self.name}"
            if limit is not None:
                sql += f" LIMIT {limit}"
                if offset is not None:
                    sql += f" OFFSET {offset}"

            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                results = cursor.fetchall()
                columns = [description[0] for description in cursor.description]
                return [QuerySet(dict(zip(columns, row)), self.path, self.name) for row in results]
        except SQLiteError as e:
            raise DatabaseError(f"Error fetching records: {str(e)}")

    def filter(self, *args, **kwargs) -> List[QuerySet]:
        if not args and not kwargs:
            raise ValidationError("Minimum one argument must be provided.")

        try:
            or_stmt = {}
            like_stmt = {}
            like_or_stmt = {}
            order_by = {}
            limit = None
            offset = None

            for arg in args:
                if isinstance(arg, dict):
                    if "OR" in arg:
                        or_stmt = arg["OR"]
                    if "LIKE" in arg:
                        like_stmt = arg["LIKE"]
                    if "LIKE-OR" in arg:
                        like_or_stmt = arg["LIKE-OR"]["OR"]
                    if "ORDER BY" in arg:
                        order_by = arg
                    if "LIMIT" in arg:
                        limit = arg["LIMIT"]
                    if "OFFSET" in arg:
                        offset = arg["OFFSET"]

            where_clauses = []

            def format_value(value):
                if value is None:
                    return "NULL"
                if isinstance(value, (int, float)):
                    return str(value)
                return f"'{self._escape_string(str(value))}'"

            for key, value in or_stmt.items():
                if isinstance(value, list):
                    or_conditions = [f"{key} = {format_value(item)}" for item in value]
                    where_clauses.append(f"({' OR '.join(or_conditions)})")
                else:
                    where_clauses.append(f"{key} = {format_value(value)}")

            for key, value in like_or_stmt.items():
                if isinstance(value, list):
                    like_conditions = [f"{key} LIKE '%{self._escape_string(str(item))}%'" for item in value]
                    where_clauses.append(f"({' OR '.join(like_conditions)})")
                else:
                    where_clauses.append(f"{key} LIKE '%{self._escape_string(str(value))}%'")

            for key, value in like_stmt.items():
                if isinstance(value, list):
                    like_conditions = [f"{key} LIKE '%{self._escape_string(str(item))}%'" for item in value]
                    where_clauses.append(f"({' OR '.join(like_conditions)})")
                else:
                    where_clauses.append(f"{key} LIKE '%{self._escape_string(str(value))}%'")

            for key, value in kwargs.items():
                where_clauses.append(f"{key} = {format_value(value)}")

            sql = f"SELECT * FROM {self.name}"
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)

            order_by_cols = order_by.get("ORDER BY", [])
            desc = order_by.get("DESCENDING", False)
            if order_by_cols:
                sql += " ORDER BY " + ", ".join(order_by_cols)
                if desc:
                    sql += " DESC"

            if limit is not None:
                sql += f" LIMIT {limit}"
                if offset is not None:
                    sql += f" OFFSET {offset}"

            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                results = cursor.fetchall()
                columns = [description[0] for description in cursor.description]
                return [QuerySet(dict(zip(columns, row)), self.path, self.name) for row in results]
        except SQLiteError as e:
            raise DatabaseError(f"Error filtering records: {str(e)}")

    def insert(self, **kwargs):
        keys = ", ".join(kwargs.keys())
        values = ", ".join(
            [
                str(value) if isinstance(value, (int, float)) else f"'{value}'"
                for value in kwargs.values()
            ]
        )
        sql = f"INSERT INTO {self.name} ({keys}) VALUES ({values})"
        self.db.execute(sql)

    def clear(self):
        self.db.execute(f"DELETE FROM {self.name}")

    def delete(self, *args, **kwargs):
        if not args and not kwargs:
            raise TypeError(
                "You should provide minimum one argument or else entire table data will get deleted. Please use the clear() function to do that."
            )

        or_stmt = {}
        like_stmt = {}
        like_or_stmt = {}

        for arg in args:
            if "OR" in arg:
                or_stmt = arg["OR"]
            if "LIKE" in arg:
                like_stmt = arg["LIKE"]
            if "LIKE-OR" in arg:
                like_or_stmt = arg["LIKE-OR"]["OR"]

        where_clauses = []

        def format_value(value):
            return str(value) if isinstance(value, (int, float)) else f"'{value}'"

        for key, value in or_stmt.items():
            if isinstance(value, list):
                or_conditions = [f"{key} = {format_value(item)}" for item in value]
                where_clauses.append(f"({' OR '.join(or_conditions)})")
            else:
                where_clauses.append(f"{key} = {format_value(value)}")

        for key, value in like_or_stmt.items():
            if isinstance(value, list):
                like_conditions = [f"{key} LIKE '%{item}%'" for item in value]
                where_clauses.append(f"({' OR '.join(like_conditions)})")
            else:
                where_clauses.append(f"{key} LIKE '%{value}%'")

        for key, value in like_stmt.items():
            if isinstance(value, list):
                like_conditions = [f"{key} LIKE '%{item}%'" for item in value]
                where_clauses.append(f"({' OR '.join(like_conditions)})")
            else:
                where_clauses.append(f"{key} LIKE '%{value}%'")

        for key, value in kwargs.items():
            where_clauses.append(f"{key} = {format_value(value)}")

        sql = f"DELETE FROM {self.name}"
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        self.db.execute(sql)

    def toCSV(self):
        data = self.db.execute(f"SELECT * FROM {self.name}")
        if not data:
            raise ValueError(f"There is no data in the {self.name} table.")

        with open(f"{self.name}.csv", "w", newline="") as fh:
            writer = csv.writer(fh)
            columns = data[0].keys()
            writer.writerow(columns)
            for row in data:
                writer.writerow([row[col] for col in columns])

    def toJSON(self):
        data = self.db.execute(f"SELECT * FROM {self.name}")
        if not data:
            raise ValueError(f"There is no data in the {self.name} table.")

        with open(f"{self.name}.json", "w") as outfile:
            json.dump({self.name: data}, outfile)

    def count(self):
        return len(self.db.execute(f"SELECT * FROM {self.name}"))

    def create_index(self, name: str, columns: List[str], unique: bool = False) -> None:
        """Create an index on specified columns"""
        if not isinstance(name, str):
            raise ValidationError("Index name must be a string")
        if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
            raise ValidationError("Columns must be a list of strings")
        if not columns:
            raise ValidationError("At least one column must be specified")

        try:
            unique_str = "UNIQUE" if unique else ""
            columns_str = ", ".join(columns)
            sql = f"CREATE {unique_str} INDEX {name} ON {self.name} ({columns_str})"

            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                conn.commit()
        except SQLiteError as e:
            raise DatabaseError(f"Error creating index: {str(e)}")

    def drop_index(self, name: str) -> None:
        """Drop an index by name"""
        if not isinstance(name, str):
            raise ValidationError("Index name must be a string")

        try:
            sql = f"DROP INDEX IF EXISTS {name}"
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                conn.commit()
        except SQLiteError as e:
            raise DatabaseError(f"Error dropping index: {str(e)}")

    @contextmanager
    def transaction(self):
        """Context manager for database transactions"""
        conn = None
        try:
            conn = sqlite3.connect(self.path)
            conn.execute("BEGIN TRANSACTION")
            yield conn
            conn.commit()
        except SQLiteError as e:
            if conn:
                conn.rollback()
            raise DatabaseError(f"Transaction error: {str(e)}")
        finally:
            if conn:
                conn.close()

    def addForeignKey(self, name: str, references: str, on_delete: str = "CASCADE") -> None:
        """Add a foreign key constraint to the table"""
        if not isinstance(name, str):
            raise ValidationError("Column name must be a string")
        if not isinstance(references, str):
            raise ValidationError("References must be a string")
        if on_delete not in ["CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT", "NO ACTION"]:
            raise ValidationError("Invalid on_delete action")

        try:
            sql = f"FOREIGN KEY ({name}) REFERENCES {references} ON DELETE {on_delete}"
            self.sql = self.sql[:-1]
            if self.sql[-1] != "(":
                self.sql += ", "
            self.sql += sql + ")"
        except Exception as e:
            raise DatabaseError(f"Error adding foreign key: {str(e)}")

    def addUniqueConstraint(self, columns: List[str]) -> None:
        """Add a unique constraint to the table"""
        if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
            raise ValidationError("Columns must be a list of strings")
        if not columns:
            raise ValidationError("At least one column must be specified")

        try:
            columns_str = ", ".join(columns)
            sql = f"UNIQUE ({columns_str})"
            self.sql = self.sql[:-1]
            if self.sql[-1] != "(":
                self.sql += ", "
            self.sql += sql + ")"
        except Exception as e:
            raise DatabaseError(f"Error adding unique constraint: {str(e)}")

    def bulk_insert(self, records: List[Dict[str, Any]]) -> None:
        """Insert multiple records at once"""
        if not isinstance(records, list):
            raise ValidationError("Records must be a list of dictionaries")
        if not records:
            return

        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                columns = records[0].keys()
                placeholders = ", ".join(["?" for _ in columns])
                sql = f"INSERT INTO {self.name} ({', '.join(columns)}) VALUES ({placeholders})"

                for record in records:
                    values = [record.get(col) for col in columns]
                    cursor.execute(sql, values)
        except SQLiteError as e:
            raise DatabaseError(f"Error in bulk insert: {str(e)}")

    def bulk_update(self, records: List[Dict[str, Any]], key_column: str) -> None:
        """Update multiple records at once"""
        if not isinstance(records, list):
            raise ValidationError("Records must be a list of dictionaries")
        if not records:
            return
        if key_column not in records[0]:
            raise ValidationError(f"Key column {key_column} not found in records")

        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                for record in records:
                    set_clauses = []
                    values = []
                    for key, value in record.items():
                        if key != key_column:
                            set_clauses.append(f"{key} = ?")
                            values.append(value)
                    values.append(record[key_column])

                    sql = f"UPDATE {self.name} SET {', '.join(set_clauses)} WHERE {key_column} = ?"
                    cursor.execute(sql, values)
        except SQLiteError as e:
            raise DatabaseError(f"Error in bulk update: {str(e)}")

    def bulk_delete(self, key_values: List[Any], key_column: str) -> None:
        """Delete multiple records at once"""
        if not isinstance(key_values, list):
            raise ValidationError("Key values must be a list")
        if not key_values:
            return

        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                placeholders = ", ".join(["?" for _ in key_values])
                sql = f"DELETE FROM {self.name} WHERE {key_column} IN ({placeholders})"
                cursor.execute(sql, key_values)
        except SQLiteError as e:
            raise DatabaseError(f"Error in bulk delete: {str(e)}")

    def exists(self, **kwargs) -> bool:
        """Check if a record exists matching the given criteria"""
        try:
            where_clauses = []
            for key, value in kwargs.items():
                if value is None:
                    where_clauses.append(f"{key} IS NULL")
                elif isinstance(value, (int, float)):
                    where_clauses.append(f"{key} = {value}")
                else:
                    where_clauses.append(f"{key} = '{self._escape_string(str(value))}'")

            sql = f"SELECT 1 FROM {self.name}"
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)
            sql += " LIMIT 1"

            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                return cursor.fetchone() is not None
        except SQLiteError as e:
            raise DatabaseError(f"Error checking existence: {str(e)}")

    def count(self, **kwargs) -> int:
        """Count records matching the given criteria"""
        try:
            where_clauses = []
            for key, value in kwargs.items():
                if value is None:
                    where_clauses.append(f"{key} IS NULL")
                elif isinstance(value, (int, float)):
                    where_clauses.append(f"{key} = {value}")
                else:
                    where_clauses.append(f"{key} = '{self._escape_string(str(value))}'")

            sql = f"SELECT COUNT(*) FROM {self.name}"
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)

            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                return cursor.fetchone()[0]
        except SQLiteError as e:
            raise DatabaseError(f"Error counting records: {str(e)}")

    def get_or_create(self, defaults: Dict[str, Any] = None, **kwargs) -> tuple[QuerySet, bool]:
        """Get a record or create it if it doesn't exist"""
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                where_clauses = []
                for key, value in kwargs.items():
                    if value is None:
                        where_clauses.append(f"{key} IS NULL")
                    elif isinstance(value, (int, float)):
                        where_clauses.append(f"{key} = {value}")
                    else:
                        where_clauses.append(f"{key} = '{self._escape_string(str(value))}'")

                sql = f"SELECT * FROM {self.name}"
                if where_clauses:
                    sql += " WHERE " + " AND ".join(where_clauses)
                sql += " LIMIT 1"

                cursor.execute(sql)
                result = cursor.fetchone()

                if result:
                    columns = [description[0] for description in cursor.description]
                    return QuerySet(dict(zip(columns, result)), self.path, self.name), False

                # Create new record
                if defaults:
                    kwargs.update(defaults)

                columns = list(kwargs.keys())
                values = list(kwargs.values())
                placeholders = ", ".join(["?" for _ in values])
                sql = f"INSERT INTO {self.name} ({', '.join(columns)}) VALUES ({placeholders})"

                cursor.execute(sql, values)
                conn.commit()

                # Get the created record
                cursor.execute(f"SELECT * FROM {self.name} WHERE rowid = last_insert_rowid()")
                result = cursor.fetchone()
                columns = [description[0] for description in cursor.description]
                return QuerySet(dict(zip(columns, result)), self.path, self.name), True
        except SQLiteError as e:
            raise DatabaseError(f"Error in get_or_create: {str(e)}")

def OR(**kwargs):
    return {"OR": kwargs}


def LIKE(*args, **kwargs):
    if args:
        return {"LIKE-OR": {"OR": args[0]}, "LIKE": kwargs}
    else:
        return {"LIKE": kwargs}


def ORDER_BY(*args, descending=False):
    if not args:
        raise ValueError("You need to pass at least one column name.")
    return {"ORDER BY": args, "DESCENDING": descending}
