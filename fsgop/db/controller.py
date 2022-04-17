from typing import Iterable, Dict, List, Optional, Union, Iterator, Tuple
from difflib import SequenceMatcher
from collections import namedtuple
from pathlib import Path
from datetime import datetime

from .record import Record
from .repository import Repository
from .person import Person
from .vehicle import Vehicle
from .mission import Mission, ONE_SEATED_TRAINING, TWO_SEATED_TRAINING
from .mission import NORMAL_FLIGHT, GUEST_FLIGHT, CHECK_FLIGHT

Match = namedtuple("Match", ["index", "rec1", "rec2"])

CREW = [
    "pilot",
    "copilot",
    "passenger1",
    "passenger2",
    "passenger3",
    "passenger4"
]


class Controller(object):
    def __init__(self, repo: Union[Repository, str, Path]) -> None:
        if isinstance(repo, Repository):
            self._repo = repo
        else:
            self._repo = Repository(db=repo)

    def __enter__(self) -> "Controller":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._repo is not None:
            self._repo.__exit__(exc_type, exc_val, exc_tb)

    def missions(self, **kwargs) -> Iterator[Mission]:
        """Select missions from the database

        Args:
            **kwargs: Keyword arguments passed verbatim to
                :meth:`fsgop.db.Repository.read`

        Yields:
            Complete Mission records matching the query
        """
        yield from self._repo.add("person_properties",
                                  self._repo.add(
                                      "vehicle_properties",
                                      self._repo.read("missions", **kwargs)))

    def vehicles(self, **kwargs) -> Iterator[Vehicle]:
        """Select vehicles from the database

        Args:
            **kwargs: Keyword arguments passed verbatim to
                :meth:`fsgop.db.Repository.read`

        Yields:
            Complete Vehicle records matching the query
        """
        yield from self._repo.add("vehicle_properties",
                                  self._repo.read("vehicles", **kwargs))

    def missions_of(self,
                    person: Person,
                    since: Optional[datetime] = None,
                    until: Optional[datetime] = None) -> Iterator[Mission]:
        """Select all missions of a given person inside a given time interval

        Args:
            person: Person for which to select flights. If no uid is specified,
                a search for this person in the database is performed.
            since: Only flights departed on or after this time are shown. If
                ``None``, flights since the beginning of time are selected.
            until: Only flights departed before this datetime instance are
                selected. If ``None``, all flights until the end of time are
                shown.

        Yields:
            One Mission instance per matching flight
        """
        if person.uid is None:
            person = next(self._repo.find([person]))

        cols = [f"missions.{s}" for s in CREW]
        where = f"({person.uid} IN ({','.join(cols)}))"
        if since is not None or until is not None:
            fmt = self._repo.schema["missions"].get_column("begin").fmt
            for d, op in ((since, ">="), (until, "<")):
                if d is not None:
                    where = f"{where} AND missions.begin{op}'{d.strftime(fmt)}'"
        yield from self._repo.add("person_properties",
                                  self._repo.add(
                                      "vehicle_properties",
                                      self._repo.read("missions", where=where)))

    def missions_like(self, m: Mission, where: str = "") -> Iterator[Mission]:
        """Find missions which are similar to a given mission

        A mission is defined as *similar* here, if either at least one crew
        member or the aircraft are engaged in two overlapping missions. Two
        similar missions cannot physically occur in reality.

        Args:
            m: Reference mission
            where: String containing additional search criteria, which are added
                to the WHERE clause.

        Yields:
             One Mission instance per similar mission to m in the database.
        """
        if m.begin is None or m.end is None:
            return
        w1 = f"(missions.begin<'{m.end}' AND missions.end>'{m.begin}')"
        crew = ",".join(f"missions.{s}" for s in CREW)
        w2 = " OR ".join(f"{int(getattr(m, s))} IN ({crew})"
                         for s in CREW if getattr(m, s))
        if m.vehicle is not None:
            w3 = f"missions.vehicle={int(m.vehicle)}"
            w2 = f"{w2} OR {w3}" if w2 else w3
        _where = f"{w1} AND ({w2})"
        where = f"({where}) AND {_where}" if where else _where
        yield from self._repo.select("missions", where=where)

    def conflicts(self) -> Iterator[Tuple[Mission, List[Mission]]]:
        """Iterate over missions with inconsistent data

        Yields:
            Tuple containing the mission causing the conflict and a list
            containing the associated conflicting missions
        """
        where = "begin IS NOT NULL AND end IS NOT NULL"
        for mission in self._repo.select("missions", where=where, order="uid"):
            tmp = list(self.missions_like(mission, where=f"uid>{mission.uid}"))
            if tmp:
                yield mission, tmp

    def incomplete(self,
                   categories: Optional[Iterable[int]] = None,
                   fields: Optional[Iterable[str]] = None) -> Iterator[Mission]:
        """Iterate over missions with incomplete data

        Args:
            categories: Numeric identifiers of flight categories to check. Only
                categories named here are checked. Defaults to

                - NORMAL_FLIGHT
                - GUEST_FLIGHT
                - CHECK_FLIGHT
                - ONE_SEATED_TRAINING
                - TWO_SEATED_TRAINING
            fields: Mandatory fields in mission, which must not be ``None``.
                Defaults to

                - pilot
                - vehicle
                - begin
                - end
                - origin
                - destination
                - launch

        Yields:
            One Mission instance per incomplete mission dataset
        """
        _fields = (
            "pilot",
            "vehicle",
            "begin",
            "end",
            "origin",
            "destination",
            "launch"
        )
        _categories = (
            NORMAL_FLIGHT,
            GUEST_FLIGHT,
            CHECK_FLIGHT,
            ONE_SEATED_TRAINING,
            TWO_SEATED_TRAINING
        )

        if fields is None:
            fields = _fields

        if categories is None:
            categories = _categories

        where = " OR ".join(f"{x} IS NULL" for x in fields)
        cats = ",".join(f"{int(x)}" for x in categories)
        where = f"({where}) AND category IN ({cats})"
        yield from self._repo.select("missions", where=where)

    def diff(self,
             path: Union[Path, str],
             table: Optional[str] = None,
             threshold: Optional[float] = 0.,
             **kwargs) -> List[Match]:
        """Get difference of file data with database data

        Args:
            path: Path to input (csv, xlsx) file containing tabulated data
            table: Name of associated database table. Not required, if basename
                of path coincides with table name
            threshold: Float value setting the minimum similarity. Defaults to
                0.8
            **kwargs: Keyword arguments passed verbatim to
                :meth:`fsgop.db.Repository.read_file`.

        Returns:
             List containing Match instances
        """
        if not table:
            table = Path(path).stem

        return self.match(
            self._repo.read_file(path, table=table, **kwargs),
            self._repo.select(table),
            threshold=threshold
        )

    @staticmethod
    def index(recs: Iterable[Record]) -> Dict[str, Record]:
        """Index Records using their index tuple

        Args:
            recs: Iterable of Records

        Returns:
            Dictionary containing a string representation of the index tuple
            as key and the associated Record instance as value

        Raises:
            ValueError: If one of the records in recs does not contain sufficient
                information to create an index tuple
            ValueError: If two or more records share the same index tuple
        """
        retval = dict()
        for j, rec in enumerate(recs):
            t = rec.index_tuple()
            if t is None:
                raise ValueError(f"Unable to index record {j} ({rec})")
            key = "".join(map(str, t)).replace(" ", "")
            if retval.setdefault(key, rec) != rec:
                raise ValueError(f"Key {key} for record {rec} "
                                 "is not unique")
        return retval

    @staticmethod
    def match(l1: Iterable[Record],
              l2: Iterable[Record],
              threshold: float = 0.) -> List[Match]:
        """Match records from one sequence to those of another using index tuples

        Attempts to match each record in sequence 1 to a matching record in
        sequence 2. Records are matched based on their respective index tuples.

        Args:
            l1: Iterable of records to compare
            l2: Other iterable of records
            threshold: Float value in the range [0., 1] controlling match
                accuracy. A value of 1. will require exact matches, while a
                value of zero will attempt to always find a match.

        Returns:
            Iterable of tuples. Each tuple contains:
            - a string representation of the index tuple
            - The associated record of sequence 1, or ``None`` if no record in
              sequence 1 could be associated with this index tuple
            - The associated record of sequence 2, or ``None`` if no record in
              sequence 2 could be associated with this index tuple
        """
        assert 0. <= threshold <= 1.
        idx1 = Controller.index(l1)
        idx2 = Controller.index(l2)
        matches = dict()
        for idx, rec in idx1.items():
            match_rec = idx2.pop(idx, None)
            if match_rec is not None:
                matches[idx] = (rec, match_rec)

        unmatched = idx1.keys() - matches.keys()
        idx1 = {k: idx1[k] for k in unmatched}

        if idx1 and idx2 and threshold < 1.:
            # try to find additional matches using difflib
            diff = SequenceMatcher(autojunk=False)
            for idx, rec in idx1.items():
                if not idx2:
                    break
                diff.set_seq2(idx)
                # "or" in next line allows execution of two statements
                tmp = max((diff.set_seq1(k) or diff.ratio(), k, v)
                          for k, v in idx2.items())
                if tmp[0] >= threshold:
                    matches[idx] = (rec, tmp[2])
                    idx2.pop(tmp[1])

            unmatched = idx1.keys() - matches.keys()
            idx1 = {k: idx1[k] for k in unmatched}

        retval = [Match(k, v, None) for k, v in idx1.items()]
        retval.extend(Match(k, None, v) for k, v in idx2.items())
        retval.extend(Match(k, v1, v2) for k, (v1, v2) in matches.items())
        return retval