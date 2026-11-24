"""A small DataFrame stand-in for the stand-in programs of the untracked runners: tables are lists of dictionaries, and the few calls that the load steps make
(filter, select, withColumn, groupBy, orderBy, limit, unionByName, writeTo, count and collect) work on them. No Spark is imported."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any


class Column:
    """An expression over one row."""

    def __init__(self, fn: Callable[[dict[str, Any]], Any], name: str | None = None) -> None:
        self.fn, self.name = fn, name

    def cast(self, kind: str) -> Column:
        def cast_value(row: dict[str, Any]) -> Any:
            value = self.fn(row)
            if value is None:
                return None
            return int(value) if kind in ("bigint", "int") else str(value)

        return Column(cast_value, self.name)

    def isin(self, *values: Any) -> Column:
        wanted = values[0] if len(values) == 1 and isinstance(values[0], list | tuple | set) else values
        return Column(lambda row: self.fn(row) in wanted)

    def alias(self, name: str) -> Column:
        return Column(self.fn, name)

    def __eq__(self, other: object) -> Column:  # type: ignore[override]
        return Column(lambda row: self.fn(row) == (other.fn(row) if isinstance(other, Column) else other))

    def __add__(self, other: Column) -> Column:
        return Column(lambda row: self.fn(row) + other.fn(row))

    def __and__(self, other: Column) -> Column:
        return Column(lambda row: bool(self.fn(row)) and bool(other.fn(row)))

    def __or__(self, other: Column) -> Column:
        return Column(lambda row: bool(self.fn(row)) or bool(other.fn(row)))

    def __invert__(self) -> Column:
        return Column(lambda row: not self.fn(row))

    __hash__ = None  # type: ignore[assignment]


class When:
    def __init__(self, condition: Column, value: Column) -> None:
        self.condition, self.value = condition, value

    def otherwise(self, other: Column) -> Column:
        return Column(lambda row: self.value.fn(row) if self.condition.fn(row) else other.fn(row))


class Count:
    """``count(...)`` as an aggregate of ``groupBy(...).agg(...)``."""

    def __init__(self, name: str = "count") -> None:
        self.name = name

    def alias(self, name: str) -> Count:
        return Count(name)


class Functions:
    @staticmethod
    def col(name: str) -> Column:
        return Column(lambda row: row[name], name)

    @staticmethod
    def lit(value: Any) -> Column:
        return Column(lambda row: value)

    @staticmethod
    def when(condition: Column, value: Column) -> When:
        return When(condition, value)

    @staticmethod
    def count(_: Column) -> Count:
        return Count()


class Grouped:
    def __init__(self, frame: Frame, keys: list[Column]) -> None:
        self.frame, self.rows, self.keys = frame, frame.rows, keys

    def agg(self, *aggregates: Count) -> Frame:
        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in self.rows:
            groups.setdefault(tuple(key.fn(row) for key in self.keys), []).append(row)
        names = [key.name or "key" for key in self.keys]
        found = [{**dict(zip(names, values, strict=True)), **{item.name: len(rows) for item in aggregates}} for values, rows in groups.items()]
        return self.frame.derived(found, [*names, *(item.name for item in aggregates)])


class Writer:
    def __init__(self, session: Session, name: str, frame: Frame) -> None:
        self.session, self.name, self.frame = session, name, frame

    def tableProperty(self, key: str, value: str) -> Writer:
        return self

    def create(self) -> None:
        if self.name in self.session.tables:
            raise RuntimeError("the table is already there")
        self.session.tables[self.name] = Frame(self.frame.rows, list(self.frame.columns))

    def append(self) -> None:
        held = self.session.tables[self.name]
        self.session.tables[self.name] = Frame([*held.rows, *self.frame.rows], held.columns or list(self.frame.columns))

    def overwritePartitions(self) -> None:
        self.session.tables[self.name] = Frame(self.frame.rows, list(self.frame.columns))


class Frame:
    session: Session

    def __init__(self, rows: Iterable[dict[str, Any]], columns: list[str] | None = None, session: Session | None = None) -> None:
        self.rows = [dict(row) for row in rows]
        self.columns = columns if columns is not None else (list(self.rows[0]) if self.rows else [])
        if session is not None:
            self.session = session

    def derived(self, rows: Iterable[dict[str, Any]], columns: list[str] | None = None) -> Frame:
        return Frame(rows, columns if columns is not None else self.columns, getattr(self, "session", None))

    def filter(self, condition: Column) -> Frame:
        return self.derived([row for row in self.rows if condition.fn(row)])

    def select(self, *items: str | Column) -> Frame:
        names = [item if isinstance(item, str) else (item.name or "column") for item in items]
        found = [{name: row[item] if isinstance(item, str) else item.fn(row) for name, item in zip(names, items, strict=True)} for row in self.rows]
        return self.derived(found, names)

    def withColumn(self, name: str, value: Column) -> Frame:
        return self.derived([{**row, name: value.fn(row)} for row in self.rows], [*self.columns, *([] if name in self.columns else [name])])

    def orderBy(self, *names: str) -> Frame:
        return self.derived(sorted(self.rows, key=lambda row: tuple((row[name] is None, row[name]) for name in names)))

    def limit(self, count: int) -> Frame:
        return self.derived(self.rows[:count])

    def unionByName(self, other: Frame) -> Frame:
        return self.derived([*self.rows, *other.rows])

    def coalesce(self, _: int) -> Frame:
        return self

    def distinct(self) -> Frame:
        seen: list[dict[str, Any]] = []
        for row in self.rows:
            if row not in seen:
                seen.append(row)
        return self.derived(seen)

    def groupBy(self, *keys: Column) -> Grouped:
        return Grouped(self, list(keys))

    def toDF(self, *names: str) -> Frame:
        return self.derived([dict(zip(names, row.values(), strict=True)) for row in self.rows], list(names))

    def collect(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.rows]

    def count(self) -> int:
        return len(self.rows)

    def writeTo(self, name: str) -> Writer:
        return Writer(self.session, name, self)


class Reader:
    def __init__(self, session: Session) -> None:
        self.session = session

    def option(self, key: str, value: str) -> Reader:
        return self

    def csv(self, path: str) -> Frame:
        rows, columns = self.session.files[path]
        return Frame(rows, list(columns), self.session)


class Session:
    """Tables by name and files by path. Every frame that it hands out writes back to it."""

    def __init__(self, files: dict[str, tuple[list[dict[str, Any]], list[str]]] | None = None, tables: dict[str, Frame] | None = None) -> None:
        self.files = files or {}
        self.tables: dict[str, Frame] = {name: self.attach(frame) for name, frame in (tables or {}).items()}

    def attach(self, frame: Frame) -> Frame:
        frame.session = self
        return frame

    def table(self, name: str) -> Frame:
        held = self.tables[name]
        return self.attach(Frame(held.rows, list(held.columns)))

    def createDataFrame(self, rows: list[dict[str, Any]], schema: Any = None) -> Frame:
        return self.attach(Frame(rows))

    def sql(self, query: Any) -> Frame | None:
        if isinstance(query, tuple) and query[0] == "values":
            return self.attach(Frame(query[1]))
        return None

    @property
    def read(self) -> Reader:
        return Reader(self)
