from typing import Optional
from .record import Record, to

SINGLE_ENGINE_PISTON = 1
ULTRA_LIGHT = 2
TOURING_MOTOR_GLIDER = 3
MOTOR_GLIDER = 4
GLIDER = 5

WINCH = 11
CAR = 12


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
                 comments: Optional[str] = None) -> None:
        super().__init__(uid=uid)
        self.manufacturer = to(str, manufacturer, default=None)
        self.model = to(str, model, default=None)
        self.serial_number = to(str, serial_number, default=None)
        self.num_seats = to(int, num_seats, default=1)
        self.category = to(int, category, default=None)
        self.comments = to(str, comments, default=None)
