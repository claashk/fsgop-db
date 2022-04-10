# -*- coding: utf-8 -*-
from typing import Optional, Iterable, Dict, Callable, Generator, Any, Union
from pathlib import Path
from csv import writer as csv_writer

from .record import Record
from .table_io import Xlsx2Csv


class SpreadsheetView(object):
    """Writes Records to spreadsheets

    Args:
        columns: Iterable of strings defining the columns in their respective
            order of appearance. Nested attributes are specified using a dot
            (".") as separator.
        path: Path to output file. If not set in constructor, it has to be set
            directly in attribute path before invoking the writer!
        headings: Dictionary using heading name (as specified in columns) as key
            and the associated heading string as value. Use ``None`` or the emtpy
            string to disable a heading for a given column. If all headings are
            emtpy, then no heading line will be printed.
        fmt: Dictionary using heading name as key and a callable as value. The
            callable will be invoked to convert the respective record attribute
            to a string. ``str`` is used as default formatter for columns not
            specified here.
        **kwargs: Options passed verbatim to csvwriter.
    """
    def __init__(self,
                 columns: Iterable[str],
                 path: Optional[Union[str, Path]] = None,
                 headings: Optional[Dict[str, str]] = None,
                 fmt: Optional[Dict[str, Callable]] = None,
                 **kwargs) -> None:
        self._columns = []
        self._formatters = []
        self._headings = []
        self.path = Path(path) if path is not None else None
        self.opts = kwargs.copy()

        if headings is None:
            headings = dict()

        if fmt is None:
            fmt = dict()

        for col in columns:
            self._columns.append(col.split("."))
            self._formatters.append(fmt.get(col, str))
            self._headings.append(headings.get(col, col))

    def __call__(self, recs: Iterable[Record]):
        if not self.path:
            #TODO -> or write to stdout ?
            raise ValueError("Output path not specified")
        if self.path.suffix == ".xlsx":
            with Xlsx2Csv(self.path, read_only=False) as writer:
                self._write(recs, writer)
        else:
            with open(self.path.with_suffix(".csv"), 'w', newline='') as ofile:
                writer = csv_writer(ofile, **self.opts)
                self._write(recs, writer)

    @property
    def header(self) -> Generator[tuple, None, None]:
        """Get header lines

        Yields:
            Header lines as tuples
        """
        line = tuple(s if s is not None else "" for s in self._headings)
        if any(line):
            yield line
        return

    def _write(self, recs, writer):
        for line in self.header:
            writer.writerow(line)

        for rec in recs:
            writer.writerow(
                tuple(fmt(self._getattr(rec, cols))
                      for cols, fmt in zip(self._columns, self._formatters)))

    @staticmethod
    def _getattr(obj: Any, attrs: Iterable[str], *args) -> Any:
        """Get attributes recursively from object

        Args:
            obj: Any object, from which to extract a recursive attribute
            attrs: Iterable of strings specifying the attributes in the order
                the shall be retrieved (``["first", "second", "third"] will
                return ``obj.first.second.third``)
            *args: Only used to specify a default value returned if an attribute
                is not found

        Return:
            Attribute of obj
        """
        retval = obj
        for attr in attrs:
            if retval is None:
                break
            retval = getattr(retval, attr, *args)
        return retval

    @staticmethod
    def date_formatter(fmt: str) -> Callable:
        """Get formatter for datetime objects

        Args:
            fmt: Format string in notation used by
                :meth:`datetime.datetime.strftime`

        Return:
            Callable accepting a datetime object as input and returning a string
            with the specifed format.
        """
        return lambda x: x.strftime(fmt)
