from datetime import datetime, timedelta
from typing import Union, Optional, Set

from .record import Record, to
from .vehicle import Vehicle
from .person import Person
from .launch_method import LaunchMethod

NORMAL_FLIGHT = 1
GUEST_FLIGHT = 2
ONE_SEATED_TRAINING = 11
TWO_SEATED_TRAINING = 12
WINCH_OPERATION = 21

CHARGE_PILOT = 1
CHARGE_COPILOT = 2
CHARGE_GUEST = 3
CHARGE_PILOT_AND_COPILOT = 3


class Journey(Record):
    """Native data model for a flight or other journey of a vehicle
    
    Args:
        uid: Unique numeric ID of this journey.
        vehicle: Vehicle used for this journey. An integer will be interpreted as
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
        category: Journey category
        num_trips: Number of landings/winch launches
        launch_method: Launch method. An integer will be interpreted as uid.
        launch_journey: Tow-flight or winch mission period.
        departure_location: Departure location
        departure_utc: Departure time
        arrival_location: Landing location
        arrival_utc: Landing time
        off_block_utc: Offblock UTC
        on_block_utc: On-block UTC
        engine_hours_upon_departure: Engine hours (Hobbs meter reading)
        engine_hours_upon_arrival: Engine hours upon arrival
        charge_person: Member to charge or a negative constant
        comments (str): Eventual comments
    """
    index = ["vehicle", "departure_utc", "arrival_utc"]

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
            num_trips: Optional[int] = None,
            launch_method: Optional[Union[int, LaunchMethod]] = None,
            launch_journey: Optional[Union[int, "Journey"]] = None,
            departure_location: Optional[str] = None,
            departure_utc: Optional[Union[str, datetime]] = None,
            arrival_location: Optional[str] = None,
            arrival_utc: Optional[Union[str, datetime]] = None,
            off_block_utc: Optional[Union[str, datetime]] = None,
            on_block_utc: Optional[Union[str, datetime]] = None,
            engine_hours_upon_departure: Optional[Union[str, timedelta]] = None,
            engine_hours_upon_arrival: Optional[Union[str, timedelta]] = None,
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
        self.num_trips = to(int, num_trips, default=1)
        self.launch_method = to(LaunchMethod, launch_method)
        self.launch_journey = to(Journey, launch_journey)
        self.departure_location = to(str, departure_location)
        self.departure_utc = to(datetime, departure_utc)
        self.arrival_location = to(str, arrival_location)
        self.arrival_utc = to(datetime, arrival_utc, default=None)
        self.off_block_utc = to(datetime, off_block_utc, default=None)
        self.on_block_utc = to(datetime, on_block_utc, default=None)
        self.engine_hours_upon_departure = to(timedelta,
                                              engine_hours_upon_departure,
                                              default=None)
        self.engine_hours_upon_arrival = to(timedelta,
                                            engine_hours_upon_arrival,
                                            default=None)
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
        return self.arrival_utc - self.departure_utc

    def almost_equal(self, other: "Journey") -> bool:
        """Check if two flights are conflicting.
        
        Two flights are similar, if and only if their flight times overlap and
        either a crew member or the plane are identical.
        
        Args:
            other: Journey to compare to
        
        Returns:
            ``True`` if and only if `self` and `other` are similar
        """
        for x in (self, other):
            if None in (x.arrival_utc,
                        x.departure_utc,
                        x.vehicle,
                        x.pilot):
                raise ValueError(f"{x} is incomplete")

        return all((
            self.arrival_utc >= other.departure_utc,
            self.departure_utc <= other.arrival_utc,
            any((bool(self.crew() & other.crew()),
                 self.vehicle == other.vehicle))
        ))
