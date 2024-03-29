from datetime import datetime, timedelta, time
from typing import Union, Optional, Set, Iterable

from .record import Record, to
from .vehicle import Vehicle
from .person import Person

#  EASA PART FCL distinguishes the following flight types:
#  -> dual flight instruction: Flugunterricht mit FLuglehrer (student with FI)
#  -> (supervised) solo: Alleinflug unter Aufsicht (student without FI)
#  -> solo cross-country: Allein-Überlandflug
#  -> dual cross-country: Überlandflug mit Fluglehrer
#  -> training flights: Schulungsflüge (license holders with FI)
#  -> skill test: Praktische Prüfung (student with FE)
#  -> proficiency check: Befähigungsüberprüfung (license holder with FE)
#  -> assessment of competence: Beurteilung der Kompetenz
#    (FI, proficiency check for instructors)
#
#  Translations verbatim from German and English versions of EU reg. 2020/358
#  ref. e.g. SFCL.130, SFCL.160
#
#  References:
#  https://eur-lex.europa.eu/legal-content/de/TXT/?uri=CELEX:32020R0358
#  https://www.easa.europa.eu/sites/default/files/dfu/Easy_Access_Rules_for_Part-FCL-Aug20.pdf
#  https://www.easa.europa.eu/sites/default/files/dfu/Sailplane%20Rule%20Book.pdf

NORMAL_FLIGHT = 1
PASSENGER_FLIGHT = 2
TRAINING_FLIGHT = 3
AEROTOW = 4
PROFICIENCY_CHECK = 5

SUPERVISED_SOLO = 11
DUAL_INSTRUCTION = 12
SOLO_CROSS_COUNTRY = 13
DUAL_CROSS_COUNTRY = 14
SKILL_TEST = 15

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
        num_stints: Number of landings or winch launches
        launch: Tow-flight or winch mission period. A string representing a
            launch method ('FS', 'W') will result in a generic launch method of
            the specified kind.
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
    winch_launch_keys = {"WS", "W"}
    aerotow_keys = {"AT", "FS", "F"}
    self_launch_keys = {"SL", "ES", "E"}
    categories = {
        "normal flight": NORMAL_FLIGHT,
        "aerotow": AEROTOW,
        "passenger flight": PASSENGER_FLIGHT,
        "proficiency check": PROFICIENCY_CHECK,
        "training flight": TRAINING_FLIGHT,
        "supervised solo": SUPERVISED_SOLO,
        "dual flight instruction": DUAL_INSTRUCTION,
        "solo cross-country": SOLO_CROSS_COUNTRY,
        "dual cross-country": DUAL_CROSS_COUNTRY,
        "skill test": SKILL_TEST,
        "winch session": WINCH_OPERATION
    }

    copilot_is_pic = {
        TRAINING_FLIGHT,
        PROFICIENCY_CHECK,
        DUAL_INSTRUCTION,
        DUAL_CROSS_COUNTRY,
        SKILL_TEST
    }

    pilot_requires_licence = {
        NORMAL_FLIGHT,
        PASSENGER_FLIGHT,
        TRAINING_FLIGHT,
        PROFICIENCY_CHECK
    }

    copilot_requires_fi_licence = {
        PROFICIENCY_CHECK,
        TRAINING_FLIGHT,
        DUAL_INSTRUCTION,
        DUAL_CROSS_COUNTRY
    }

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
            category: Optional[Union[str, int]] = None,
            num_stints: Optional[int] = None,
            launch: Optional[Union[int, str, "Mission"]] = None,
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
        self.vehicle = to(Vehicle, vehicle, default=None)
        self.pilot = to(Person, pilot, default=None)
        self.copilot = to(Person, copilot, default=None)
        self.passenger1 = to(Person, passenger1, default=None)
        self.passenger2 = to(Person, passenger2, default=None)
        self.passenger3 = to(Person, passenger3, default=None)
        self.passenger4 = to(Person, passenger4, default=None)
        if isinstance(category, str):
            self.category = self.categories[category]
        else:
            self.category = to(int, category, default=NORMAL_FLIGHT)
        self.num_stints = to(int, num_stints, default=1)
        self.origin = to(str, origin, default=None)
        self.begin = to(datetime, begin, default=None)
        self.destination = to(str, destination, default=None)
        self.end = to(datetime, end, default=None)
        self.off_block_utc = to(datetime, off_block_utc, default=None)
        self.on_block_utc = to(datetime, on_block_utc, default=None)
        hrs = []
        for x in (engine_hours_begin, engine_hours_end):
            dt = to(timedelta, x, default=None)
            hrs.append(dt.total_seconds() / 3600. if dt is not None else None)
        self.engine_hours_begin, self.engine_hours_end = hrs
        self.charge_person = to(Person, charge_person, default=self.pilot)
        self.comments = to(str, comments, default=None)

        if isinstance(launch, str):
            if launch in self.winch_launch_keys:
                self.launch = self.winch_launch_for(self)
            elif launch in self.aerotow_keys:
                self.launch = self.aerotow_for(self)
            elif launch in self.self_launch_keys:
                self.launch = self
            else:
                raise ValueError(f"Invalid launch method string: '{launch}'")
        else:
            self.launch = to(Mission, launch, default=self)
            if self.launch.key() == self.key():
                self.launch = self  # avoid recursion

    @property
    def pic(self) -> Person:
        """Returns pilot in command
        """
        if self.category in self.copilot_is_pic:
            return self.copilot
        return self.pilot

    @property
    def crew(self) -> Set[Person]:
        """Returns the crew of this journey as set"""
        return {self.pilot,
                self.copilot,
                self.passenger1,
                self.passenger2,
                self.passenger3,
                self.passenger4} - {Person(), None}
        
    def duration(self) -> timedelta:
        """Returns the duration of the journey
        """
        return self.end - self.begin

    def has_generic_launch(self) -> bool:
        """A mission has a generic launch, iff no launch vehicle is specified

        Return:
            ``True`` if and only if the launch of this mission is generic
        """
        if self.launch is None:
            return False

        return (self.launch.category in [WINCH_OPERATION, AEROTOW]
                and self.launch.vehicle is None)

    def is_aerotow(self) -> bool:
        """Check if the current mission is a towflight for another mission
        """
        return self.category == AEROTOW

    def is_matching_aerotow_for(self, mission: "Mission") -> bool:
        """Check if this mission could be an aerotow for another mission

        Checks if this mission instance could be an aerotow for another mission.

        Args:
            mission: Potentially towed mission

        Return:
            True if this mission could be the aerotow for ``mission``
        """
        if not self.is_aerotow():
            return False

        if mission.begin != self.begin or mission.origin != self.origin:
            return False

        if mission.launch is not None and not mission.launch.is_aerotow():
            return False

        if mission.vehicle is not None and not mission.vehicle.is_glider():
            return False

        return True

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
            any((bool(self.crew & other.crew),
                 self.vehicle == other.vehicle))
        ))

    @property
    def licence_warnings(self) -> Set[str]:
        """Check for missing licences of pilot and copilot

        Return:
            set containing error messages
        """
        msg = set()
        if (self.pilot is not None
            and self.category in self.pilot_requires_licence
            and not self.pilot.holds_licence(self.vehicle.pic_licences,
                                             when=self.begin)):
            msg.add("Pilot missing valid licence")
        if self.category in self.copilot_requires_fi_licence:
            if self.copilot is None:
                msg.add("Missing FI/copilot")
            elif not self.copilot.holds_licence(self.vehicle.instructor_licences,
                                                when=self.begin):
                msg.add("Copilot missing FI licence")
        return msg

    def category_warnings(self) -> Set[str]:
        msg = set()
        if (self.pilot is not None
            and self.category != PASSENGER_FLIGHT
            and self.pilot.holds_licence(self.vehicle.instructor_licences,
                                         when=self.begin)):
            msg.add("Possibly mis-categorised instruction")
        if (self.category in (SUPERVISED_SOLO, SOLO_CROSS_COUNTRY)
            and self.copilot is not None):
            msg.add("Solo flight must not include co-pilot")
        return msg

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
            kwargs = Person.layout(prefix=f"{prefix}{p}_", allow=allow)
            if kwargs:
                retval[p] = kwargs

        kwargs = Vehicle.layout(prefix=f"{prefix}vehicle_", allow=allow)
        if kwargs:
            retval["vehicle"] = kwargs

        if not prefix.endswith("launch_"):
            kwargs = cls.layout(prefix=f"{prefix}launch_", allow=allow)
            if kwargs:
                retval["launch"] = kwargs
        return retval

    @classmethod
    def aerotow_for(cls,
                    mission: "Mission",
                    vehicle: Optional[Vehicle] = None,
                    pilot: Optional[Person] = None,
                    destination: Optional[str] = None,
                    end: Optional[datetime] = None) -> "Mission":
        """Create an aerotow for a mission

        Args:
            mission: Mission for which to create an aerotow
            vehicle: Vehicle used for aerotow
            pilot: Tow-pilot
            destination: Landing location of towflight
            end: landing time of tow flight

        Return:
            Mission representing the aerotow
        """
        if mission.uid is not None:
            comment = f"Auto-generated aerotow for flight {mission.uid}"
        else:
            comment = f"Auto-generated aerotow for flight {mission}"
        m = cls(uid=None,
                vehicle=vehicle,
                pilot=pilot,
                category=AEROTOW,
                num_stints=mission.num_stints,
                origin=mission.origin,
                begin=mission.begin,
                destination=destination,
                end=end,
                charge_person=mission.pilot,
                comments=comment)
        m.launch = m
        return m

    @classmethod
    def winch_launch_for(cls,
                         mission: "Mission",
                         operator: Optional[Person] = None,
                         vehicle: Optional[Vehicle] = None,
                         begin: Optional[datetime] = None,
                         end: Optional[datetime] = None) -> "Mission":
        """Create a new winch launch mission

        We suppose that one winch mission spans one day at one location.

        Args:
            mission: Mission for which to create a winch launch
            operator: Winch operator
            vehicle: Vehicle (winch) used for this winch mission
            begin: Begin of this winch mission. If not provided, it is set to
                midnight of the day of the mission.
            end: End of this winch mission. If not provided, it is set to
                midnight of the day following begin.

        Return:
            Mission representing the winch launch. Uid is not set.
        """
        # create a new mission
        if begin is None:
            begin = mission.begin
        if begin is None and mission.end is not None:
            begin = datetime.combine(mission.end.date(),
                                     time(hour=0, minute=0, second=0))
        if begin is not None and end is None:
            end = datetime.combine((begin + timedelta(hours=24)).date(),
                                   time(hour=0, minute=0, second=0))

        if mission.uid is not None:
            comment = f"Auto-generated winch session for mission {mission.uid}"
        else:
            comment = f"Auto-generated winch session for mission {mission}"
        m = cls(uid=None,  # has to be set later by user
                vehicle=vehicle,
                pilot=operator,
                category=WINCH_OPERATION,
                num_stints=None,
                origin=mission.origin,
                begin=begin,
                destination=mission.origin,  # winch does not move
                end=end,
                charge_person=None,
                comments=comment)
        return m
