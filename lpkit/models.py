from datetime import time, datetime
from pytz import timezone
from lpkit.classes import ReportStatus, StoreStatus, WeekDay
from lpkit.utils import DateUtils

class Table:
    def __init__(self):
        self.__synced__:dict[str, any] = {}

    def column(self, name:str, rowDict):
        try:
            self.__synced__[name] = rowDict[name]
        except:
            self.__synced__[name] = None
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
        Table.__init__(self)
        self.storeId:int = self.column(PolledStat._StoreId, row)
        self.timestampUtc = DateUtils.toDateTime(self.column(PolledStat._TimestampUtc, row))
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
        Table.__init__(self)
        self.storeId:int = self.column(StoreSchedule._StoreId, row)
        self.dayOfWeek:WeekDay = WeekDay(self.column(StoreSchedule._DayOfWeek, row))
        self.lStartTime:time = DateUtils.toTime(self.column(StoreSchedule._LStartTime, row))
        self.lEndTime:time = DateUtils.toTime(self.column(StoreSchedule._LEndTime, row))
    
    @staticmethod
    def dummyFullDaySchedule(storeId:int, dayOfWeek:WeekDay):
        schedule = StoreSchedule(row={
            StoreSchedule._StoreId: storeId,
            StoreSchedule._DayOfWeek: dayOfWeek.value,
            StoreSchedule._LStartTime: "00:00:00.0",
            StoreSchedule._LEndTime: "23:59:59.9999"
        })
        
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
        Table.__init__(self)
        self.storeId:int = self.column(StoreInfo._StoreId, row)
        self.onBoardedAt:int = self.column(StoreInfo._OnBoardedAt, row) or 0
        try:
            self.timezone = timezone(self.column(StoreInfo._Timezone, row))
        except:
            self.timezone = None


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
        Table.__init__(self)
        self.id:str = self.column(ReportStat._Id, row)
        self.version:int = self.column(ReportStat._Version, row)
        self.status = ReportStatus(self.column(ReportStat._Status, row))
        self.runAt:int = self.column(ReportStat._RunAt, row)
        self.completedAt:int|None = self.column(ReportStat._CompletedAt, row)