from typing import Optional, Union, Iterable, Set
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
    categories = {
        "single engine piston": SINGLE_ENGINE_PISTON,
        "ultralight": ULTRALIGHT,
        "touring motor glider": TOURING_MOTOR_GLIDER,
        "motor glider": MOTOR_GLIDER,
        "glider": GLIDER,
        "winch": WINCH,
        "car": CAR,
        "undefined": UNDEFINED
    }

    accepted_pic_licences = {
        SINGLE_ENGINE_PISTON: {"PPL(A)-SEP"},
        ULTRALIGHT: {"PPL(A)", "LAPL(A)"},
        TOURING_MOTOR_GLIDER: {"SPL-TMG", "PPL(A)-TMG", "LAPL(A)-TMG", "LAPL(S)-TMG"},
        GLIDER: {"SPL", "LAPL(S)"},
        WINCH: {"WINDENSCHEIN"}
    }

    accepted_instructor_licences = {
        SINGLE_ENGINE_PISTON: {"FI(A)-SEP"},
        ULTRALIGHT: {"FI(A)"},
        TOURING_MOTOR_GLIDER: {"FI(A)-TMG", "FI(S)-TMG"},
        GLIDER: {"FI(S)"}
    }

    def __init__(self,
                 uid: Optional[int] = None,
                 manufacturer: Optional[str] = None,
                 model: Optional[str] = None,
                 serial_number: Optional[str] = None,
                 num_seats: Optional[int] = None,
                 category: Optional[Union[int, str]] = None,
                 registration: Optional[Union["VehicleProperty", str]] = None,
                 comments: Optional[str] = None) -> None:
        super().__init__(uid=uid)
        self.manufacturer = to(str, manufacturer, default=None)
        self.model = to(str, model, default=None)
        self.serial_number = to(str, serial_number, default=None)
        self.num_seats = to(int, num_seats, default=1)
        if isinstance(category, str):
            self.category = self.categories[category]
        else:
            self.category = to(int, category, default=None)
        self.comments = to(str, comments, default=None)

        if registration:
            if isinstance(registration, str):
                reg = VehicleProperty(value=registration)
            else:
                reg = registration
            reg.kind = "registration"
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

    @property
    def pic_licences(self) -> Set[str]:
        """Get list of accepted licences for vehicle operation

        Return:
            Set of strings containing licence types permitting operation
        """
        return self.accepted_pic_licences[self.category]

    @property
    def instructor_licences(self) -> Set[str]:
        """Get list of licences qualifying for instruction on this vehicle

        Args:
            lic: Licence property

        Return:
            ``True`` if and only if the licence *lic*
        """
        return self.accepted_instructor_licences[self.category]

    def is_glider(self) -> bool:
        """Check if vehicle is a glider

        Returns:
            ``True`` if and only if the vehicle is either a glider or a motor
            glider (not a touring motor glider)
        """
        return (self.category is not None
                and self.category in (GLIDER, MOTOR_GLIDER))


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
        kind: Name of this property
        value: Property value
    """
    index = [x if x != "rec" else "vehicle" for x in Property.index]

    def __init__(self,
                 uid: Optional[int] = None,
                 vehicle: Optional[Union[Vehicle, int]] = None,
                 valid_from: Optional[datetime] = None,
                 valid_until: Optional[datetime] = None,
                 kind: Optional[str] = None,
                 value: Optional[str] = None) -> None:
        super().__init__(uid=uid,
                         rec=to(Vehicle, vehicle, default=None),
                         valid_from=valid_from,
                         valid_until=valid_until,
                         kind=kind,
                         value=value)

    @property
    def vehicle(self):
        return self.rec

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
        return cls._layout_helper(Vehicle, prefix=prefix, allow=allow)
