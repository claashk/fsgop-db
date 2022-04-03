from typing import Optional, Union
from datetime import datetime
from .record import Record, to
from .property import Property

SINGLE_ENGINE_PISTON = 1
ULTRALIGHT = 2
TOURING_MOTOR_GLIDER = 3
MOTOR_GLIDER = 4
GLIDER = 5

WINCH = 11
CAR = 12
UNDEFINED = 9999  # use this value only to indicate errors/warnings


class Vehicle(Record):
    """Native vehicle model
    
    Args:
        uid: Unique numeric ID of this vehicle record
        manufacturer: Name of airplane manufacturer
        model: Model of airplane
        serial_number: Unique airplane serial number
        num_seats: Number of seats
        category: Category. One of the values in CATEGORIES.
        comments: any comment
    """
    index = ["manufacturer", "serial_number"]

    def __init__(self,
                 uid: Optional[int] = None,
                 manufacturer: Optional[str] = None,
                 model: Optional[str] = None,
                 serial_number: Optional[str] = None,
                 num_seats: Optional[int] = None,
                 category: Optional[int] = None,
                 registration: Optional[Union["VehicleProperty", str]] = None,
                 comments: Optional[str] = None) -> None:
        super().__init__(uid=uid)
        self.manufacturer = to(str, manufacturer, default=None)
        self.model = to(str, model, default=None)
        self.serial_number = to(str, serial_number, default=None)
        self.num_seats = to(int, num_seats, default=1)
        self.category = to(int, category, default=None)
        self.comments = to(str, comments, default=None)

        if registration:
            if isinstance(registration, str):
                reg = VehicleProperty(value=registration)
            else:
                reg = registration
            reg.name = "registration"
            reg.add_to(self)

    @property
    def registration(self) -> Optional[str]:
        """Get registration of this vehicle

        Returns:
            Registration or ``None``, if no registration is defined

        Raises:
            ValueError if more than one registration property exists
        """
        regs = self["registration"]
        if len(regs) > 1:
            raise ValueError(
                f"In vehicle {self}: Found multiple registrations: "
                f"{', '.join(x.value for x in regs)}")
        for p in regs:
            return p.value
        return None


class VehicleProperty(Property):
    """Vehicle property implementation

    Arguments:
        uid: Unique ID of this property record
        vehicle: Vehicle this property describes. An integer is interpreted as
            uid.
        valid_from: Date from which on this property is valid. ``None`` if it is
           valid since the dawn of time
        valid_until: Date after which this property expires. Use ``None`` to
           indicate that the property does not expire
        name: Name of this property
        value: Property value
    """
    def __init__(self,
                 uid: Optional[int] = None,
                 vehicle: Optional[Union[Vehicle, int]] = None,
                 valid_from: Optional[datetime] = None,
                 valid_until: Optional[datetime] = None,
                 name: Optional[str] = None,
                 value: Optional[str] = None) -> None:
        super().__init__(uid=uid,
                         rec=to(Vehicle, vehicle, default=None),
                         valid_from=valid_from,
                         valid_until=valid_until,
                         name=name,
                         value=value)

    @property
    def vehicle(self):
        return self.rec

