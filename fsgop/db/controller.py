from typing import Iterable, Dict, List, Optional, Union, Callable
from difflib import SequenceMatcher
from collections import namedtuple
from pathlib import Path

from .record import Record
from .repository import Repository

Match = namedtuple("Match", ["index", "rec1", "rec2"])


class Controller(object):
    def __init__(self, repo: Optional[Repository] = None) -> None:
        self._repo = repo

    def diff(self,
             path: Union[Path, str],
             table: Optional[str] = None,
             threshold: Optional[float] = 0.,
             **kwargs) -> List[Match]:
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