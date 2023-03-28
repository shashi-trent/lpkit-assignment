import traceback
import pytz
from os.path import exists as osPathExists
from lpkit.classes import StoreReport, UtcEpochRange
from lpkit.configkeeper import ConfigKeeper
from lpkit.repos import *
from lpkit.utils import DateUtils, WeekDay, SubReportType

# @param(localSchedules) : all schedules inside it must be w.r.t same timezone
def filterSchedulesInUtcEpochRange(fromUtcEpoch:int, toUtcEpoch:int, localSchedules:list[StoreSchedule], refWeekStartUtcEpoch:int):
    filteredEpochRanges:list[tuple[int,int]] = []

    localSchedules.sort(key=lambda x:str(x.lStartTime))
    for schedule in localSchedules:
        schStartUtcEpoch = DateUtils.toUtcEpoch(schedule.dayOfWeek, schedule.lStartTime, refWeekStartUtcEpoch)
        schEndUtcEpoch = DateUtils.toUtcEpoch(schedule.dayOfWeek, schedule.lEndTime, refWeekStartUtcEpoch)

        leftEpoch = max(schStartUtcEpoch, fromUtcEpoch)
        rightEpoch = min(schEndUtcEpoch, toUtcEpoch)

        if rightEpoch > leftEpoch:
            thisEpochRange:tuple[int,int] = leftEpoch, rightEpoch

            if len(filteredEpochRanges) == 0:
                filteredEpochRanges.append(thisEpochRange)
            else:
                lastEpochRange = filteredEpochRanges[-1]
                if lastEpochRange[1] >= thisEpochRange[0]:
                    lastEpochRange = lastEpochRange[0], max(lastEpochRange[1], thisEpochRange[1])
                    filteredEpochRanges[-1] = lastEpochRange
    
    return filteredEpochRanges

def calculateUpDownHours(utcEpochRangesInput:list[tuple[int,int]], sortedPolledStats:list[PolledStat], ignoreTill:int, pivotUtcEpoch:int):
    upDownMillis:list[int] = [0, 0]

    # modified time-ranges w.r.t ignoreTill value provided
    utcEpochRanges:list[tuple[int,int]] = []
    for idx in range(0, len(utcEpochRangesInput)):
        if utcEpochRangesInput[idx][1] > ignoreTill:
            partlyApplicableTupleModified = max(utcEpochRangesInput[idx][0], ignoreTill), utcEpochRangesInput[idx][1]
            utcEpochRanges.append(partlyApplicableTupleModified)
            utcEpochRanges += utcEpochRangesInput[idx+1:]
            break

    if len(utcEpochRanges) == 0:
        return 0, 0

    # last (or default-last) polledStat properties for use in iteration
    lastTimeRangeIdx:int = 0
    lastPollUtcEpoch:int = 0
    lastPollStatus:StoreStatus | None = None

    # internal method to add time-diff w.r.t 
    #       1. utcEpochRanges(=StoreSchedule) {fromIdx:toIdx}
    #       2. epochMillis-difference {fromEpoch:toEpoch}
    #       3. considering status inside calculated-time-diff = @param(status = None has no effect on UpDown-time currently)
    def addEpochFilteredMillisDiff(fromIdx:int, toIdx:int, fromEpoch:float, toEpoch:float, status:StoreStatus|None):
        millisDiff:int = 0
        while fromIdx < toIdx:
            epochRange = utcEpochRanges[fromIdx]
            if toEpoch < epochRange[0]:
                break
            millisDiff += min(toEpoch, epochRange[1]) - max(fromEpoch, epochRange[0])
            if toEpoch < epochRange[1]:
                break
            fromIdx += 1
        if status == StoreStatus.Active:
            upDownMillis[0] += millisDiff
        elif status == StoreStatus.InActive:
            upDownMillis[1] += millisDiff
        return fromIdx

    for statIdx in range(len(sortedPolledStats)):
        stat = sortedPolledStats[statIdx]
        pollUtcEpoch:int = round(stat.timestampUtc.timestamp() * 1000)

        if pollUtcEpoch >= ignoreTill:
            curTimeRangeIdx = lastTimeRangeIdx
            while curTimeRangeIdx < len(utcEpochRanges):
                if utcEpochRanges[curTimeRangeIdx][1] <= pollUtcEpoch:
                    curTimeRangeIdx += 1
                else:
                    break

            # midEpoch divides last & cur polledStats into 2 equal time-ranges to assign status (active or inactive) to
            midEpoch = (lastPollUtcEpoch + pollUtcEpoch) / 2
            
            # calculate time diff from lastPollStat to middleTimeOf(lastPollStat, curPollStat)
            rangeIdx:int = addEpochFilteredMillisDiff(lastTimeRangeIdx, min(curTimeRangeIdx + 1, len(utcEpochRanges)), lastPollUtcEpoch, midEpoch, lastPollStatus)

            if curTimeRangeIdx < len(utcEpochRanges) and utcEpochRanges[curTimeRangeIdx][0] <= pollUtcEpoch:
                # calculate time diff from middleTimeOf(lastPollStat, curPollStat) to curPollStat
                rangeIdx = addEpochFilteredMillisDiff(rangeIdx, curTimeRangeIdx + 1, midEpoch, pollUtcEpoch, stat.status)

                # if it is last-polledStat -> 
                #       use pivotUtcEpoch(=epochOf when report generation started) as next polledStat(with status=None) to extrapolate
                if statIdx == len(sortedPolledStats)-1:
                    extrapolateToEpoch = (pivotUtcEpoch + pollUtcEpoch) / 2
                    rangeIdx = addEpochFilteredMillisDiff(curTimeRangeIdx, len(utcEpochRanges), pollUtcEpoch, extrapolateToEpoch, stat.status)
            else:
                break

            lastTimeRangeIdx = curTimeRangeIdx
            lastPollStatus = stat.status
            lastPollUtcEpoch = pollUtcEpoch
    
    return upDownMillis[0]/DateUtils.HOUR_TO_MILLIS, upDownMillis[1]/DateUtils.HOUR_TO_MILLIS

class ReportService:
    @staticmethod
    def generate(reportStat:ReportStat, pivotEpoch:int|None=None):
        try:
            reportFilePath = f"{ConfigKeeper.getReportFolderPath()}report-{reportStat.id}-v{reportStat.version}.csv"
            
            # already existing : no effect
            if osPathExists(reportFilePath):
                return
            
            pivotEpoch = pivotEpoch or reportStat.runAt
            
            with open(reportFilePath, "w") as reportFile:
                reportFile.writelines([ReportService.getCsvHeader()+"\n"])

                pollStatBatchSize = ConfigKeeper.asInt('POLL_STAT_DBFETCH_BATCH_SIZE')

                batchSize = ConfigKeeper.asInt("STORE_TZS_DBFETCH_BATCH_SIZE")
                batchNo = 0
                while True:
                    stores = StoreRepo.getTzSortedStores(skip=batchNo*batchSize, limit=batchSize)
                    if len(stores) == 0:
                        break

                    lastHourUpDownHours = ReportService.getSubReportStoreHours(stores, pivotEpoch, SubReportType.LastHour, 
                                                                            max(1, pollStatBatchSize//3))
                    lastDayUpDownHours = ReportService.getSubReportStoreHours(stores, pivotEpoch, SubReportType.LastDay, 
                                                                            max(1, pollStatBatchSize//24))
                    lastWeekUpDownHours = ReportService.getSubReportStoreHours(stores, pivotEpoch, SubReportType.LastWeek, 
                                                                            max(1, pollStatBatchSize//24))
                    
                    reportFormatList = [
                        StoreReport(store.storeId, lastHourUpDownHours[store.storeId][0], lastDayUpDownHours[store.storeId][0],
                                    lastWeekUpDownHours[store.storeId][0], lastHourUpDownHours[store.storeId][1],
                                    lastDayUpDownHours[store.storeId][1], lastWeekUpDownHours[store.storeId][1])
                        for store in stores
                    ]

                    reportFile.writelines([ReportService.getCsvText(storeReport)+"\n" for storeReport in reportFormatList])
                    reportFile.flush()

                    batchNo = batchNo + 1
            
            reportStat.status = ReportStatus.Complete
            reportStat.completedAt = DateUtils.curUtcEpoch()
            ReportRepo.saveStat(reportStat)
        except Exception as e:
            reportStat.status = ReportStatus.Error
            reportStat.completedAt = DateUtils.curUtcEpoch()
            ReportRepo.saveStat(reportStat)
            print(f"exception={e}")
            traceback.print_exc()
    

    @staticmethod
    def getSubReportStoreHours(stores:list[StoreInfo], pivotUtcEpoch, subReportType:SubReportType, pollBatchSize:int):
        upDownHoursDict:dict[int, tuple[float,float]] = {}

        tz:str = ""
        subReportTiming = UtcEpochRange(fromEpoch=0, toEpoch=0, lPytzTimezone=pytz.utc)
        batch:list[StoreInfo] = []

        def runAndClearCurBatch():
            upDownHoursBatchDict = ReportService.getSameTzStoresUpDownHours(subReportTiming, batch, pivotUtcEpoch)
            upDownHoursDict.update(upDownHoursBatchDict)
            batch.clear()

        for store in stores:
            if store.storeId not in upDownHoursDict:
                upDownHoursDict[store.storeId] = (0, 0)

            if store.timezone is None:
                store.timezone = timezone("America/Chicago")
            
            if tz != str(store.timezone):
                if len(batch) > 0:
                    runAndClearCurBatch()
                tz = str(store.timezone)
                subReportTiming = DateUtils.subReportTiming(store.timezone, pivotUtcEpoch, subReportType)
            elif len(batch) >= pollBatchSize:
                runAndClearCurBatch()

            batch.append(store)

        if len(batch) > 0:
            runAndClearCurBatch()

        return upDownHoursDict
    

    @staticmethod
    def getSameTzStoresUpDownHours(subReportTiming:UtcEpochRange, stores:list[StoreInfo], pivotUtcEpoch:int):
        upDownHoursDict:dict[int, tuple[float, float]] = {}

        schedulesDict = StoreRepo.getStoreSchedules([store.storeId for store in stores], subReportTiming.lWeekDay)

        concernedWeekDays:list[WeekDay] = [subReportTiming.lWeekDay] if subReportTiming.lWeekDay is not None else [wDay for wDay in WeekDay]
        refWeekStartEpoch = DateUtils.getWeekStartUtcEpoch(subReportTiming.fromEpoch, subReportTiming.lPytzTimezone)
        
        for weekDay in concernedWeekDays:
            scheduledStoreIds:list[int] = []
            filteredEpochRangesDict:dict[int,list[tuple[int,int]]] = {}

            validPollEpochRange = UtcEpochRange(fromEpoch=pivotUtcEpoch, toEpoch=0, lPytzTimezone=pytz.utc)
            for store in stores:
                filteredEpochRanges = filterSchedulesInUtcEpochRange(subReportTiming.fromEpoch, subReportTiming.toEpoch, 
                                                schedulesDict[store.storeId][weekDay], refWeekStartEpoch)
                # if store is scheduled in concerned time range
                if len(filteredEpochRanges) != 0:
                    filteredEpochRangesDict[store.storeId] = filteredEpochRanges
                    scheduledStoreIds.append(store.storeId)
                    validPollEpochRange.fromEpoch = min(filteredEpochRanges[0][0], validPollEpochRange.fromEpoch)
                    validPollEpochRange.toEpoch = max(filteredEpochRanges[-1][1], validPollEpochRange.toEpoch)
            
            if len(scheduledStoreIds) == 0:
                continue

            sortedPolledStats = StoreRepo.getSortedPolledStats(scheduledStoreIds, validPollEpochRange.fromEpoch - DateUtils.HOUR_TO_MILLIS,
                                                               validPollEpochRange.toEpoch + DateUtils.HOUR_TO_MILLIS)
            scheduledStoreIds.clear()

            

            for store in stores:
                dayUpDownHours = calculateUpDownHours(filteredEpochRangesDict.get(store.storeId) or [], sortedPolledStats.get(store.storeId) or [], store.onBoardedAt, pivotUtcEpoch)
                if store.storeId not in upDownHoursDict:
                    upDownHoursDict[store.storeId] = dayUpDownHours
                else:
                    prvUpDownHours = upDownHoursDict[store.storeId]
                    upDownHoursDict[store.storeId] = (prvUpDownHours[0] + dayUpDownHours[0], prvUpDownHours[1] + dayUpDownHours[1])
        return upDownHoursDict

    
    @staticmethod
    def getCsvText(storeReport:StoreReport):
        upTimeTextPart = f'{round(storeReport.uptime_last_hour * 60)},{"{:.2f}".format(storeReport.uptime_last_day)},{"{:.2f}".format(storeReport.uptime_last_week)}'
        downTimeTextPart = f'{round(storeReport.downtime_last_hour * 60)},{"{:.2f}".format(storeReport.downtime_last_day)},{"{:.2f}".format(storeReport.downtime_last_week)}'
        
        return f'{storeReport.store_id},{upTimeTextPart},{downTimeTextPart}'
    
    @staticmethod
    def getCsvHeader():
        upTimeHeaderPart = "uptime_last_hour (mins),uptime_last_day (hrs),uptime_last_week (hrs)"
        downTimeHeaderPart = "downtime_last_hour (mins),downtime_last_day (hrs),downtime_last_week (hrs)"

        return f'store_id,{upTimeHeaderPart},{downTimeHeaderPart}'