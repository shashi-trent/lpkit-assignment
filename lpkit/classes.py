
import enum

class StoreStatus(enum.Enum):
    Active="active"
    InActive="inactive"

class ReportStatus(enum.Enum):
    Running=0
    Complete=1
    Error=2

class SubReportType(enum.Enum):
    LastHour=0
    LastDay=1
    LastWeek=2

class WeekDay(enum.Enum):
    Monday=0
    Tuesday=1
    Wednesday=2
    Thirsday=3
    Friday=4
    Saturday=5
    Sunday=6

class UtcEpochRange:
    # utc info
    fromEpoch:int
    toEpoch:int

    # additional local info (may not be relevant in some cases)
    lPytzTimezone = None
    lWeekDay:WeekDay|None = None

    def __init__(self, fromEpoch:int, toEpoch:int, lPytzTimezone, lWeekDay:WeekDay|None=None):
        self.fromEpoch = fromEpoch
        self.toEpoch = toEpoch
        self.lPytzTimezone = lPytzTimezone
        self.lWeekDay = lWeekDay

class StoreReport:
    def __init__(self, store_id:int, uptime_last_hour:float, uptime_last_day:float, uptime_last_week:float,
                    downtime_last_hour:float, downtime_last_day:float, downtime_last_week:float):
        self.store_id:int = store_id
        # all values are in hours
        self.uptime_last_hour:float = uptime_last_hour
        self.uptime_last_day:float = uptime_last_day
        self.uptime_last_week:float = uptime_last_week
        self.downtime_last_hour:float = downtime_last_hour
        self.downtime_last_day:float = downtime_last_day
        self.downtime_last_week:float = downtime_last_week