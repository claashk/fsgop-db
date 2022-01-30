from datetime import datetime, timedelta
from typing import Union, Optional, Set, Iterable

from .record import Record, to
from .vehicle import Vehicle
from .person import Person

NORMAL_FLIGHT = 1
GUEST_FLIGHT = 2
CHECK_FLIGHT = 3
ONE_SEATED_TRAINING = 11
TWO_SEATED_TRAINING = 12
WINCH_OPERATION = 21

CHARGE_PILOT = 1
CHARGE_COPILOT = 2
CHARGE_GUEST = 3
CHARGE_PILOT_AND_COPILOT = 3


class Mission(Record):
    """Native data model for a flight, journey or other mission of a vehicle
    
    Args:
        uid: Unique numeric ID of this journey.
        vehicle: Vehicle used for this mission. An integer will be interpreted as
          Vehicle uid.
        pilot: Person in charge of vehicle operation (PIC for regular flights,
          student pilot for two seated training). An integer will be interpreted
          as Person uid.
        copilot: Second crew member or instructor for two-seated training. An
          integer will be interpreted as Person uid.
        passenger1: Passenger
        passenger2: Passenger
        passenger3: Passenger
        passenger4: Passenger
        category: Mission category
        num_stints: Number of landings/winch launches
        launch: Tow-flight or winch mission period.
        origin: Departure location
        begin: Departure time (UTC)
        destination: Landing location
        end: Landing time (UTC)
        off_block_utc: Off-block UTC
        on_block_utc: On-block UTC
        engine_hours_begin: Engine hours (Hobbs meter reading)
        engine_hours_end: Engine hours upon arrival
        charge_person: Member to charge or a negative constant
        comments: Eventual comments
    """
    index = ["begin", "vehicle"]

    def __init__(
            self,
            uid: Optional[int] = None,
            vehicle: Optional[Union[int, Vehicle]] = None,
            pilot: Optional[Union[int, Person]] = None,
            copilot: Optional[Union[int, Person]] = None,
            passenger1: Optional[Union[int, Person]] = None,
            passenger2: Optional[Union[int, Person]] = None,
            passenger3: Optional[Union[int, Person]] = None,
            passenger4: Optional[Union[int, Person]] = None,
            category: Optional[str] = None,
            num_stints: Optional[int] = None,
            launch: Optional[Union[int, "Mission"]] = None,
            origin: Optional[str] = None,
            begin: Optional[Union[str, datetime]] = None,
            destination: Optional[str] = None,
            end: Optional[Union[str, datetime]] = None,
            off_block_utc: Optional[Union[str, datetime]] = None,
            on_block_utc: Optional[Union[str, datetime]] = None,
            engine_hours_begin: Optional[Union[str, timedelta]] = None,
            engine_hours_end: Optional[Union[str, timedelta]] = None,
            charge_person: Optional[Union[int, Person]] = None,
            comments=None) -> None:
        super().__init__(uid=uid)
        self.vehicle = to(Vehicle, vehicle)
        self.pilot = to(Person, pilot)
        self.copilot = to(Person, copilot, default=None)
        self.passenger1 = to(Person, passenger1, default=None)
        self.passenger2 = to(Person, passenger2, default=None)
        self.passenger3 = to(Person, passenger3, default=None)
        self.passenger4 = to(Person, passenger4, default=None)
        self.category = to(int, category, default=NORMAL_FLIGHT)
        self.num_stints = to(int, num_stints, default=1)
        self.launch = to(Mission, launch)
        self.origin = to(str, origin)
        self.begin = to(datetime, begin)
        self.destination = to(str, destination)
        self.end = to(datetime, end, default=None)
        self.off_block_utc = to(datetime, off_block_utc, default=None)
        self.on_block_utc = to(datetime, on_block_utc, default=None)
        hrs = []
        for x in (engine_hours_begin, engine_hours_end):
            dt = to(timedelta, x, default=None)
            if dt is not None:
                hrs.append(dt.total_seconds() / 3600.)
        self.engine_hours_begin, self.engine_hours_end = hrs
        self.charge_person = to(Person, charge_person, default=self.pilot)
        self.comments = to(str, comments, default=None)

    def pic(self) -> Person:
        """Returns pilot in command
        """
        if self.category == TWO_SEATED_TRAINING:
            return self.copilot
        return self.pilot

    def crew(self) -> Set[Person]:
        """Returns the crew of this journey as set"""
        return {self.pilot,
                self.copilot,
                self.passenger1,
                self.passenger2,
                self.passenger3,
                self.passenger4} - {Person()}
        
    def duration(self) -> timedelta:
        """Returns the duration of the journey
        """
        return self.end - self.begin

    def almost_equal(self, other: "Mission") -> bool:
        """Check if two flights are conflicting.
        
        Two flights are similar, if and only if their flight times overlap and
        either a crew member or the plane are identical.
        
        Args:
            other: Journey to compare to
        
        Returns:
            ``True`` if and only if `self` and `other` are similar
        """
        for x in (self, other):
            if None in (x.end,
                        x.begin,
                        x.vehicle,
                        x.pilot):
                raise ValueError(f"{x} is incomplete")

        return all((
            self.end >= other.begin,
            self.begin <= other.end,
            any((bool(self.crew() & other.crew()),
                 self.vehicle == other.vehicle))
        ))

    @classmethod
    def layout(cls,
               prefix: str = "",
               allow: Optional[Iterable[str]] = None) -> dict:
        """Get layout of this class

        Overrides the default implementation, which does not work for nested
        data models.

        Args:
             prefix: Prefix to add to all keys. Defaults to None
             allow: Iterable of allowed values. If not ``None``, only names in
                 this dictionary are included in the output. If a prefix is
                 provided, then values must include the prefix.

        Returns:
            Layout dictionary.
        """
        retval = super(Mission, cls).layout(prefix=prefix, allow=allow)
        for p in {"pilot",
                  "copilot",
                  "passenger1",
                  "passenger2",
                  "passenger3",
                  "passenger4",
                  "charge_person"}:
            kwargs = Person.layout(prefix=f"{p}_", allow=allow)
            if kwargs:
                retval[p] = kwargs

        kwargs = Vehicle.layout(prefix="vehicle", allow=allow)
        if kwargs:
            retval["vehicle"] = kwargs

        if not prefix.endswith("launch"):
            kwargs = cls.layout(prefix=f"{prefix}launch_", allow=allow)
            if kwargs:
                retval["launch"] = kwargs
        return retval


