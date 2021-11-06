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
    
    Arguments:
        uid (int): Unique numeric ID of this vehicle record
        manufacturer (str): Name of airplane manufacturer
        model (str): Model of airplane
        serial (str): Unique airplane serial number
        num_seats (int): Number of seats
        category (int): Category. One of the values in CATEGORIES.
        comments (str): any comment
    """
    index = ["manufacturer", "serial"]

    def __init__(self,
                 uid=None,
                 manufacturer=None,
                 model=None,
                 serial=None,
                 num_seats=None,
                 category=None,
                 comments=None):
        super().__init__(uid=uid)
        self.manufacturer = to(str, manufacturer, default="").strip()
        self.model = to(str, model, default="").strip()
        self.serial = to(str, serial, default="").strip()
        self.num_seats = to(int, num_seats, default=1)
        self.category = to(int, category, default=None)
        self.comments = to(str, comments, default="").strip()
