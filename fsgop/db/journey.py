from .record import Record, to
from datetime import datetime, timedelta
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
    
    Arguments:
        uid (int): Unique numeric ID of this journey.
        vehicle_uid (int): Unique numeric ID of vehicle used for this journey
           (ref. :doc:`Vehicle`)
        pilot_uid (int): Unique numeric ID of the person in charge of the
            vehicle operation (PIC for regular flights, student pilot for two
            seated training).
        copilot_uid (int): Unique numeric ID of the second crew member or
            instructor for two-seated training.
        passenger1_uid (int): Unique numeric ID of first passenger
        passenger2_uid (int): Unique numeric ID of second passenger
        passenger3_uid (int): Unique numeric ID of third passenger
        passenger4_uid (int): Unique numeric ID of fourth passenger
        category (str): Journey category
        num_trips (int): Number of landings/winch launches
        launch_method_uid (int): ID of applicable :class:`.LaunchMethod` record
           in table ``launch_methods`` (ref. :doc:`launch_method`)
        launch_uid (int): UID of tow-flight or winch mission period.
        departure_location (str): Departure location
        departure_utc (datetime.datetime): Departure time
        arrival_location (str): Landing location
        arrival_utc (datetime.datetime): Landing time
        off_block_utc (datetime.datetime): Offblock UTC
        on_block_utc (datetime.datetime): On-block UTC
        engine_hours_upon_departure(datetime.timedelta): Engine hours (Hobbs meter reading)
        engine_hours_upon_arrival(datetime.timedelta): Engine hours upon arrival
        charge_person_uid (int): uid of member to charge or a negative constant
        comments (str): Eventual comments
    """
    index = ["vehicle_uid", "departure_utc", "arrival_utc"]

    def __init__(self,
                 uid=None,
                 vehicle=None,
                 pilot=None,
                 copilot=None,
                 passenger1=None,
                 passenger2=None,
                 passenger3=None,
                 passenger4=None,
                 category=None,
                 num_trips=None,
                 launch_method=None,
                 launch_journey=None,
                 departure_location=None,
                 departure_utc=None,
                 arrival_location=None,
                 arrival_utc=None,
                 off_block_utc=None,
                 on_block_utc=None,
                 engine_hours_upon_departure=None,
                 engine_hours_upon_arrival=None,
                 charge_person=None,
                 comments=None):
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
        self.launch_method = to(LaunchMethod, launch_method, default=None)
        self.launch_journey = to(Journey, launch_journey, default=None)
        self.departure_location = to(str, departure_location)
        self.departure_utc = to(datetime, departure_utc)
        self.arrival_location = to(str, arrival_location)
        self.arrival_utc = to(datetime, arrival_utc, default=None)
        self.off_block_utc = to(datetime, off_block_utc, default=None)
        self.on_block_utc = to(datetime, on_block_utc, default=None)
        self.engine_hours_upon_departure = to(timedelta, engine_hours_upon_departure, default=None)
        self.engine_hours_upon_arrival = to(timedelta, engine_hours_upon_arrival, default=None)
        self.charge_person = to(Person, charge_person, default=self.pilot)
        self.comments = comments

    def pic(self):
        """Get pilot in command

        Return:
            int: ID of pilot in command
        """
        if self.category == TWO_SEATED_TRAINING:
            return self.copilot
        return self.pilot
        
    def duration(self):
        """Get flight time
        
        Return:
            datetime.timedelta : Flight time
        """
        return self.arrival_utc - self.departure_utc

    def departure_time_str(self, format=None):
        """Get departure time as string.
        
        Arguments:
            format (str): Output format (in :func:`strftime` notation).
               Defaults to :data:`~pysk.db.model.flight.TIME_FORMAT`.
        
        Return:
            str: Departure time as string in given format for local and outbound
            flights. Departure location for inbound flights.
        """
        return self.time_to_str(self.departure_time, fmt=format)

    def landing_time_str(self, format=None):
        """Get landing time as string.
        
        Arguments:
            format (str): Output format (in :func:`strftime` notation).
               Defaults to :data:`~pysk.flight.TIME_FORMAT`.
        
        Return:
            str: Landing time as string in given format for local and inbound
            flights. Landing location for outbound flights.
        """
        return self.time_to_str(self.landing_time, fmt=format)

    def conflicts_with(self, other):
        """Check if two flights are conflicting.
        
        Two flights are similar, if and only if their flight times overlap and
        either a crew member or the plane are identical.
        
        Arguments:
            other (:class:`Journey`): flight to compare to
        
        Return:
            ``True`` if and only if self and other are conflicting
        """
        if None in (self.arrival_utc,
                    self.departure_utc,
                    self.vehicle_uid,
                    self.pilot_uid):
            raise RuntimeError(f"{self} is incomplete")
        
        if None in (other.arrival_utc,
                    other.departure_utc,
                    other.vehicle_uid,
                    other.pilot_uid):
            raise RuntimeError(f"{other} is incomplete")

        return all((
            self.arrival_utc >= other.departure_utc,
            self.departure_utc <= other.arrival_utc,
            any((self.pilot_uid == other.pilot_uid,
                 self.vehicle_uid == other.vehicle_uid,
                 self.copilot_uid is not None
                 and self.copilot_uid == other.copilot_uid))))

    @staticmethod
    def time_to_str(t, fmt=None):
        if t is None:
            return ""
        return t.strftime(TIME_FORMAT if fmt is None else fmt)
