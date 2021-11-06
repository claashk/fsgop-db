from .record import Record, to
from .vehicle import Vehicle

WINCH_LAUNCH = 1
AEROTOW = 2
SELF_LAUNCH = 3


LOG_STRINGS = {
    WINCH_LAUNCH: "W",
    AEROTOW: "FS",
    SELF_LAUNCH: "ES"
}


class LaunchMethod(Record):
    """Native launch method model

    Arguments:
        uid: ID of this record in ``launch_methods`` table.
        name (str): Name of launch method
        category (int): Launch type. One of
        vehicle (:class:`~fsgop.db.Vehicle` or int): The vehicle used to launch
           other aircraft. A winch for winch starts, an aircraft for aerotow, or
           ``None`` for self-launch.
        comments (str): Any comment
    """
    index = ["manufacturer", "serial"]

    def __init__(self,
                 uid=None,
                 name=None,
                 category=None,
                 vehicle=None,
                 comments=None):
        super().__init__(uid=uid)
        self.name = to(str, name, default="")
        self.category = to(int, category, default=None)
        self.vehicle = to(Vehicle, vehicle, default=None)
        self.comments = to(str, comments, default="")
