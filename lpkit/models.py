from datetime import time, datetime
from pytz import timezone
from lpkitchen.classes import ReportStatus, StoreStatus, WeekDay

class Table:
    def __init__(self):
        self.__synced__ = {}

    def column(self, name:str, rowDict):
        self.__synced__[name] = rowDict[name]
        return self.__synced__[name]



class PolledStat(Table):
    # column names
    _StoreId = "store_id"
    _TimestampUtc = "timestamp_utc"
    _Status = "status"

    @staticmethod
    def table():
        return "store_poll_stat"
    
    def __init__(self, row):
        Table.__init__()
        self.storeId:int = self.column(PolledStat._StoreId, row)
        self.timestampUtc = datetime.strptime(self.column(PolledStat._TimestampUtc, row), "%Y-%m-%d %H:%M:%S.%f %Z")
        self.status = StoreStatus(self.column(PolledStat._Status, row))


class StoreSchedule(Table):
    # column names
    _StoreId = "store_id"
    _DayOfWeek = "dayOfWeek"
    _LStartTime = "start_time_local"
    _LEndTime = "end_time_local"

    @staticmethod
    def table():
        return "store_schedule"

    def __init__(self, row):
        Table.__init__()
        self.storeId:int = self.column(StoreSchedule._StoreId, row)
        self.dayOfWeek:WeekDay = WeekDay(self.column(StoreSchedule._DayOfWeek, row))
        self.lStartTime = time.fromisoformat(self.column(StoreSchedule._LStartTime, row))
        self.lEndTime = time.fromisoformat(self.column(StoreSchedule._LEndTime, row))
    
    @staticmethod
    def dummyFullDaySchedule(storeId:int, dayOfWeek:WeekDay):
        schedule = StoreSchedule(row={})
        schedule.storeId = storeId
        schedule.dayOfWeek = dayOfWeek
        schedule.lStartTime = time.fromisoformat("00:00:00.0")
        schedule.lEndTime =  time.fromisoformat("23:59:59.9999")
        return schedule



class StoreInfo(Table):
    # column names
    _StoreId = "store_id"
    _OnBoardedAt = "onboarded_at" # utc_epoch value
    _Timezone = "timezone_str"

    @staticmethod
    def table():
        return "store_info"

    def __init__(self, row):
        Table.__init__()
        self.storeId:int = self.column(StoreInfo._StoreId, row)
        self.onBoardedAt:int = self.column(StoreInfo._OnBoardedAt, row)
        self.timezone = timezone(self.column(StoreInfo._Timezone, row))


class ReportStat(Table):
    # column names
    _Id = "id"
    _Version = "version"
    _Status = "status"
    _RunAt = "run_at"   # utc_epoch
    _CompletedAt = "completed_at"   # utc_epoch

    @staticmethod
    def table():
        return "report_stat"

    def __init__(self, row):
        Table.__init__()
        self.id:str = self.column(ReportStat._Id, row)
        self.version:int = self.column(ReportStat._Version, row)
        self.status = ReportStatus(self.column(ReportStat._Status, row))
        self.runAt:int = self.column(ReportStat._RunAt, row)
        self.completedAt:int = self.column(ReportStat._CompletedAt, row)