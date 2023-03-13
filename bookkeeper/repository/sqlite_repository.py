import sqlite3
from typing import Any
from inspect import get_annotations
from bookkeeper.repository.abstract_repository import AbstractRepository, T


class SQLiteRepository(AbstractRepository[T]):
    def __init__(self, db_file: str, cls: type) -> None:
        self.db_file = db_file
        self.table_name = cls.__name__.lower()
        self.fields = get_annotations(cls, eval_str=True)
        self.fields.pop('pk')
        self.cls = cls

    def add(self, obj: T) -> int:
        names = ', '.join(self.fields.keys())
        p = ', '.join("?" * len(self.fields))
        values = [getattr(obj, x) for x in self.fields]
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(
                f'INSERT INTO {self.table_name} ({names}) VALUES ({p})’, values'
            )
            obj.pk = cur.lastrowid
        con.close()
        return obj.pk

    def get(self, pk: int) -> T | None:
        with sqlite3.connect(self.db_file) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute(
                f'SELECT * FROM {self.table_name} WHERE pk = ?',
                [pk]
            )
            res = cur.fetchall()
        con.close()
        if res:
            return self.cls(*res[0])
        else:
            return None

    def get_all(self, where: dict[str, Any] | None = None) -> list[T]:
        with sqlite3.connect(self.db_file) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            if where is None:
                query = "SELECT * FROM my_table"
            else:
                query = "SELECT * FROM my_table WHERE "
                for key, value in where.items():
                    query += f"{key} = ? AND "
                query = query[:-5]  # remove the last "AND"
            cur.execute(query, tuple(where.values()) if where else None)
            res = cur.fetchall()
        con.close()
        results = []
        for row in res:
            for i in range(len(self.fields)):
                obj = self.cls(*[row[i + 1]])
                obj.pk = row[0]
                results.append(obj)
        return results

    def update(self, obj: T) -> None:
        if obj.pk == 0:
            raise ValueError('attempt to update object with unknown primary key')
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            fields = obj.__dict__
            values = tuple(fields.values())
            pk = fields.pop('pk', None)
            values = values[:-1]
            set_clause = ''
            for key in fields.keys():
                set_clause += f'{key} = ?, '
            set_clause = set_clause[:-2]
            query = f"UPDATE {self.table_name} SET {set_clause} WHERE pk = ?"
            cur.execute(query, values + (pk,))
        con.close()

    def delete(self, pk: int) -> None:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            query = f"DELETE FROM {self.table_name} WHERE pk = ?"
            cur.execute(query, (pk,))
        con.close()


