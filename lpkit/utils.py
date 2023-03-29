
import pytz
from time import time as curTimestamp
from datetime import datetime, time, timedelta
from lpkit.classes import SubReportType, UtcEpochRange, WeekDay


class DateUtils:
    HOUR_TO_MILLIS = 3600000
    DAY_TO_MILLIS = 86400000
    WEEK_TO_MILLIS = 604800000

    @staticmethod
    def curUtcEpoch():
        return round(curTimestamp() * 1000) # in millis
    
    @staticmethod
    def toIsoFormat(millis:int):
        secs, _ = divmod(millis, 1000)
        mins, secs = divmod(secs, 60)
        hrs, mins = divmod(mins, 60)
        return str(time(hour=hrs, minute=mins, second=secs))
    
    @staticmethod
    def toTime(timeStr:str) -> time:
        timeStr += "" if '.' in timeStr else ".0"
        return datetime.strptime(timeStr, "%H:%M:%S.%f").time()
    
    @staticmethod
    def asUtcDateTime(dateTimeStr:str) -> datetime:
        dateTime:datetime
        try:
            dateTime = datetime.strptime(dateTimeStr, "%Y-%m-%d %H:%M:%S.%f %Z")
        except ValueError:
            dateTime = datetime.strptime(dateTimeStr, "%Y-%m-%d %H:%M:%S %Z")
        return pytz.utc.localize(dateTime)

    @staticmethod
    def getWeekStartUtcEpoch(utcEpoch:int, pytzTimezone):
        localDateTime = datetime.fromtimestamp(utcEpoch / 1000, tz=pytz.utc).astimezone(pytzTimezone)
        utcWeekStartDateTime = DateUtils.startOfWeek(localDateTime).astimezone(pytz.utc)
        return round(utcWeekStartDateTime.timestamp() * 1000)
    
    @staticmethod
    def toDateTimeIsoFormat(utcEpoch:int):
        dateTime = datetime.fromtimestamp(utcEpoch / 1000, tz=pytz.utc)
        return dateTime.isoformat(sep=" ")

    @staticmethod
    def toUtcEpoch(inputDay:WeekDay, inputTime:time, refWeekStartUtcEpoch:int) -> int:
        hourOffset = refWeekStartUtcEpoch + int(inputDay.value) * DateUtils.DAY_TO_MILLIS + inputTime.hour * DateUtils.HOUR_TO_MILLIS
        utcEpoch = hourOffset + inputTime.minute * 60000 + inputTime.second * 1000 + round(inputTime.microsecond / 1000)
        return utcEpoch
    
    @staticmethod
    def subReportTiming(pytzTimezone, referenceUtcEpoch:int, type:SubReportType):
        if type==SubReportType.LastHour:
            return DateUtils.lastHourTiming(pytzTimezone, referenceUtcEpoch)
        elif type==SubReportType.LastDay:
            return DateUtils.lastDayTiming(pytzTimezone, referenceUtcEpoch)
        elif type==SubReportType.LastWeek:
            return DateUtils.lastWeekTiming(pytzTimezone, referenceUtcEpoch)
        else:
            raise TypeError("sub-report type should not be invalid!")

    @staticmethod
    def lastHourTiming(pytzTimezone, referenceUtcEpoch:int):
        refDateTime = datetime.fromtimestamp(referenceUtcEpoch / 1000, tz=pytz.utc).astimezone(pytzTimezone)
        refWeekDayOrdinal = refDateTime.isoweekday() - 1

        toUtcEpoch = round(DateUtils.startOfHour(refDateTime).astimezone(pytz.utc).timestamp() * 1000)
        fromUtcEPoch = toUtcEpoch - DateUtils.HOUR_TO_MILLIS
        rangeWeekDayOrdinal = (7 + refWeekDayOrdinal - 1) % 7 if refDateTime.hour == 0 else refWeekDayOrdinal

        return UtcEpochRange(fromEpoch=fromUtcEPoch, toEpoch=toUtcEpoch,
                                lPytzTimezone=pytzTimezone, lWeekDay=WeekDay(rangeWeekDayOrdinal))
    
    @staticmethod
    def lastDayTiming(pytzTimezone, referenceUtcEpoch:int):
        refDateTime = datetime.fromtimestamp(referenceUtcEpoch / 1000, tz=pytz.utc).astimezone(pytzTimezone)
        refWeekDayOrdinal = refDateTime.isoweekday() - 1

        toUtcEpoch = round(DateUtils.startOfDay(refDateTime).astimezone(pytz.utc).timestamp() * 1000)
        fromUtcEPoch = toUtcEpoch - DateUtils.DAY_TO_MILLIS
        rangeWeekDayOrdinal = (7 + refWeekDayOrdinal - 1) % 7

        return UtcEpochRange(fromEpoch=fromUtcEPoch, toEpoch=toUtcEpoch,
                                lPytzTimezone=pytzTimezone, lWeekDay=WeekDay(rangeWeekDayOrdinal))
    
    @staticmethod
    def lastWeekTiming(pytzTimezone, referenceUtcEpoch):
        refDateTime = datetime.fromtimestamp(referenceUtcEpoch / 1000, tz=pytz.utc).astimezone(pytzTimezone)

        toUtcEpoch = round(DateUtils.startOfWeek(refDateTime).astimezone(pytz.utc).timestamp() * 1000)
        fromUtcEPoch = toUtcEpoch - DateUtils.WEEK_TO_MILLIS

        return UtcEpochRange(fromEpoch=fromUtcEPoch, toEpoch=toUtcEpoch, lPytzTimezone=pytzTimezone)
    
    @staticmethod
    def startOfHour(dateTime:datetime):
        return dateTime.replace(minute=0, second=0, microsecond=0)
    
    @staticmethod
    def startOfDay(dateTime:datetime):
        return dateTime.replace(hour=0, minute=0, second=0, microsecond=0)
    
    @staticmethod
    def startOfWeek(dateTime:datetime):
        curWeekDayOrdinal = dateTime.isoweekday() - 1
        return DateUtils.startOfDay(dateTime) - timedelta(days=curWeekDayOrdinal)
    
