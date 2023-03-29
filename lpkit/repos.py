from lpkit.configkeeper import ConfigKeeper
from lpkit.db import get_db
from lpkit.models import *
from lpkit.utils import WeekDay


def inQueryStr(params:list):
    if len(params) == 1:
        return f"= '{params[0]}'"
    else:
        joinedParams = ", ".join([f"'{str(param)}'" for param in params])
        return f"IN ( {joinedParams} )"


class StoreRepo:
    #
    #   @params toTimeUtc: is exculsive
    #
    @staticmethod
    def getSortedPolledStats(storeIds:list[int], fromTimeUtcEpoch:int, toTimeUtcEpoch:int):
        fromTimeUtc = DateUtils.toDateTimeIsoFormat(fromTimeUtcEpoch)
        toTimeUtc = DateUtils.toDateTimeIsoFormat(toTimeUtcEpoch)

        db = get_db()
        stats:list = []

        batchNo = 0
        batchSize = ConfigKeeper.asInt('POLL_STAT_DBFETCH_BATCH_SIZE')
        while True:
            skip = batchNo * batchSize
            _stats = db.execute(
                " ".join((
                    f"SELECT * FROM {PolledStat.table()} WHERE {PolledStat._StoreId} {inQueryStr(storeIds)} AND {PolledStat._TimestampUtc} >= '{fromTimeUtc}'",
                    f"AND {PolledStat._TimestampUtc} < '{toTimeUtc}' ORDER BY {PolledStat._TimestampUtc} LIMIT ? OFFSET ?"
                )),
                (batchSize, skip)
            ).fetchall()

            batchNo += 1

            if _stats is None:
                _stats = []
            
            if len(_stats) == 0:
                break
            else:
                stats += _stats

        sortedPolledStats:dict[int, list[PolledStat]] = {}
        for row in stats:
            stat = PolledStat(row)
            if stat.storeId not in sortedPolledStats:
                sortedPolledStats[stat.storeId] = [stat]
            else:
                sortedPolledStats[stat.storeId].append(stat)
        
        return sortedPolledStats
        

    @staticmethod
    def getStoreSchedules(storeIds:list[int], weekDay:WeekDay|None):
        db = get_db()
        schedules = db.execute(
            " ".join((
                f"SELECT * FROM {StoreSchedule.table()} WHERE {StoreSchedule._StoreId} {inQueryStr(storeIds)}",
                "" if weekDay is None else f"AND {StoreSchedule._DayOfWeek} = {weekDay.value}",
            ))
        ).fetchall()

        if schedules is None:
            schedules = []

        schedulesDict:dict[int, dict[WeekDay, list[StoreSchedule]]] = {}
        for row in schedules:
            schedule = StoreSchedule(row)
            if schedule.storeId not in schedulesDict:
                schedulesDict[schedule.storeId] = {}

            if schedule.dayOfWeek not in schedulesDict[schedule.storeId]:
                schedulesDict[schedule.storeId][schedule.dayOfWeek] = [schedule]
            else:
                schedulesDict[schedule.storeId][schedule.dayOfWeek].append(schedule)

        for storeId in storeIds:
            if storeId not in schedulesDict:
                schedulesDict[storeId] = {}
            
            if weekDay is not None:
                if weekDay not in schedulesDict[storeId]:
                    schedulesDict[storeId] = {weekDay: [StoreSchedule.dummyFullDaySchedule(storeId, weekDay)]}
            else:
                for weekDayItr in WeekDay:
                    if weekDayItr not in schedulesDict[storeId]:
                        schedulesDict[storeId][weekDayItr] = [StoreSchedule.dummyFullDaySchedule(storeId, weekDayItr)]

        return schedulesDict
    

    @staticmethod
    def getTzSortedStores(skip, limit):
        db = get_db()
        tzs = db.execute(
            f"SELECT * FROM {StoreInfo.table()} ORDER BY {StoreInfo._Timezone} LIMIT ? OFFSET ?",
            (limit, skip)
        ).fetchall()

        if tzs is None:
            tzs = []

        return [StoreInfo(row) for row in tzs]



class ReportRepo:
    @staticmethod
    def getStat(reportId:str):
        db = get_db()
        stat = db.execute(
            f"SELECT * from {ReportStat.table()} WHERE {ReportStat._Id} = '{reportId}'",
        ).fetchone()

        return None if stat is None else ReportStat(stat)
    
    @staticmethod
    def saveStat(stat:ReportStat):
        db = get_db()
        error = True
        try:
            db.execute(
                f"INSERT INTO {ReportStat.table()} ({ReportStat._Id}, {ReportStat._Status}, {ReportStat._RunAt}, {ReportStat._Version}) VALUES (?, ?, ?, ?)",
                (stat.id, stat.status.value, stat.runAt, stat.version)
            )
            db.commit()
            error = False
        except db.IntegrityError:
            db.execute(
                f"UPDATE {ReportStat.table()} SET {ReportStat._Status} = ?, {ReportStat._CompletedAt} = ? WHERE {ReportStat._Id} = ?",
                (stat.status.value, stat.completedAt, stat.id)
            )
            db.commit()
            error = False
        
        if error==True:
            raise Exception("ReportStat:save Error")
