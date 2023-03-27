import time

import pytz
from lpkitchen.classes import StoreReport, UtcEpochRange
from lpkitchen.repos import *
from lpkitchen.utils import DateUtils, WeekDay, SubReportType

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

            if filteredEpochRanges.count == 0:
                filteredEpochRanges.append(thisEpochRange)
            else:
                lastEpochRange = filteredEpochRanges[-1]
                if lastEpochRange[1] >= thisEpochRange[0]:
                    lastEpochRange[1] = max(lastEpochRange[1], thisEpochRange[1])
                    filteredEpochRanges[-1] = lastEpochRange
    
    return filteredEpochRanges

def calculateUpDownHours(utcEpochRangesInput:list[tuple[int,int]], sortedPolledStats:list[PolledStat], ignoreTill:int, pivotUtcEpoch:int):
    upDownMillis:tuple[int,int] = (0, 0)

    # modified time-ranges w.r.t ignoreTill value provided
    utcEpochRanges:list[tuple[int,int]] = []
    for idx in range(0, utcEpochRangesInput.count):
        if utcEpochRangesInput[idx][1] > ignoreTill:
            partlyApplicableTupleModified = max(utcEpochRangesInput[idx][0], ignoreTill), utcEpochRangesInput[idx][1]
            utcEpochRanges.append(partlyApplicableTupleModified)
            utcEpochRanges += utcEpochRangesInput[idx+1:]
            break

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

    for statIdx in range(sortedPolledStats.count):
        stat = sortedPolledStats[statIdx]
        pollUtcEpoch:int = DateUtils.toUtcEpoch(inputTime=stat.timestampUtc)

        if pollUtcEpoch >= ignoreTill:
            curTimeRangeIdx = lastTimeRangeIdx
            while curTimeRangeIdx < utcEpochRanges.count:
                if utcEpochRanges[curTimeRangeIdx][1] <= pollUtcEpoch:
                    curTimeRangeIdx += 1
                else:
                    break

            # midEpoch divides last & cur polledStats into 2 equal time-ranges to assign status (active or inactive) to
            midEpoch = (lastPollUtcEpoch + pollUtcEpoch) / 2
            
            # calculate time diff from lastPollStat to middleTimeOf(lastPollStat, curPollStat)
            rangeIdx:int = addEpochFilteredMillisDiff(lastTimeRangeIdx, curTimeRangeIdx + 1, lastPollUtcEpoch, midEpoch, lastPollStatus)

            if curTimeRangeIdx < utcEpochRanges.count and utcEpochRanges[curTimeRangeIdx][0] <= pollUtcEpoch:
                # calculate time diff from middleTimeOf(lastPollStat, curPollStat) to curPollStat
                rangeIdx = addEpochFilteredMillisDiff(rangeIdx, curTimeRangeIdx + 1, midEpoch, pollUtcEpoch, stat.status)

                # if it is last-polledStat -> 
                #       use pivotUtcEpoch(=epochOf when report generation started) as next polledStat(with status=None) to extrapolate
                extrapolateToEpoch = (pivotUtcEpoch + pollUtcEpoch) / 2
                rangeIdx = addEpochFilteredMillisDiff(curTimeRangeIdx, sortedPolledStats.count, pollUtcEpoch, extrapolateToEpoch, stat.status)
            else:
                break

            lastTimeRangeIdx = curTimeRangeIdx
            lastPollStatus = stat.status
            lastPollUtcEpoch = pollUtcEpoch
    
    return upDownMillis[0]/DateUtils.HOUR_TO_MILLIS, upDownMillis[1]/DateUtils.HOUR_TO_MILLIS

class ReportService:
    @staticmethod
    async def generate(reportStat:ReportStat):
        with open(f"report-{reportStat.id}-v{reportStat.version}.csv", "w") as reportFile:
            reportFile.writelines([ReportService.getCsvHeader()])

            batchSize = 25
            batchNo = 0
            while True:
                stores = StoreRepo.getTzSortedStores(skip=batchNo*batchSize, limit=batchSize)
                if stores.count == 0:
                    break

                lastHourUpDownHours = ReportService.getSubReportStoreHours(stores,
                                                            reportStat.runAt, SubReportType.LastHour, 8)
                lastDayUpDownHours = ReportService.getSubReportStoreHours(stores,
                                                            reportStat.runAt, SubReportType.LastDay, 1)
                lastWeekUpDownHours = ReportService.getSubReportStoreHours(stores,
                                                            reportStat.runAt, SubReportType.LastWeek, 1)
                
                reportFormatList = [
                    StoreReport(store.storeId, lastHourUpDownHours[store.storeId][0], lastDayUpDownHours[store.storeId][0],
                                lastWeekUpDownHours[store.storeId][0], lastHourUpDownHours[store.storeId][1],
                                lastDayUpDownHours[store.storeId][1], lastWeekUpDownHours[store.storeId][1])
                    for store in stores
                ]

                reportFile.writelines([ReportService.getCsvText(storeReport) for storeReport in reportFormatList])
                reportFile.flush()
                
                batchNo = batchNo + 1
        
        reportStat.status = ReportStatus.Complete
        reportStat.completedAt = round(time.time() * 1000)
        ReportRepo.saveStat(reportStat)
    

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
            if store.timezone is None:
                store.timezone = timezone("America/Chicago")
            
            if tz != str(store.timezone):
                if batch.count > 0:
                    runAndClearCurBatch()
                tz = str(store.timezone)
                subReportTiming = DateUtils.subReportTiming(store.timezone, pivotUtcEpoch, subReportType)
            elif batch.count >= pollBatchSize:
                runAndClearCurBatch()

            batch.append(store)

        if batch.count > 0:
            runAndClearCurBatch()

        return upDownHoursDict
    

    @staticmethod
    def getSameTzStoresUpDownHours(subReportTiming:UtcEpochRange, stores:list[StoreInfo], pivotUtcEpoch:int):
        upDownHoursDict:dict[int, tuple[float, float]] = {}

        schedulesDict = StoreRepo.getStoreSchedules([store.storeId for store in stores], subReportTiming.lWeekDay)
        
        concernedWeekDays:list[WeekDay] = [subReportTiming.lWeekDay] if subReportTiming.lWeekDay is not None else [wDay for wDay in WeekDay]
        
        for weekDay in concernedWeekDays:
            scheduledStoreIds:list[int] = []
            filteredEpochRangesDict:dict[int,list[tuple[int,int]]] = {}

            validPollEpochRange = UtcEpochRange(fromEpoch=pivotUtcEpoch, toEpoch=0, lPytzTimezone=pytz.utc)
            for store in stores:
                filteredEpochRanges = filterSchedulesInUtcEpochRange(subReportTiming.fromEpoch, subReportTiming.toEpoch, 
                                                schedulesDict[store.storeId][weekDay], subReportTiming.refWeekStartEpoch)
                # if store is scheduled in concerned time range
                if filteredEpochRanges.count != 0:
                    filteredEpochRangesDict[store.storeId] = filteredEpochRanges
                    scheduledStoreIds.append(store.storeId)
                    validPollEpochRange.fromEpoch = min(filteredEpochRanges[0][0], validPollEpochRange.fromEpoch)
                    validPollEpochRange.toEpoch = min(filteredEpochRanges[-1][1], validPollEpochRange.toEpoch)
            
            if scheduledStoreIds.count == 0:
                continue

            sortedPolledStats = StoreRepo.getSortedPolledStats(scheduledStoreIds, validPollEpochRange.fromEpoch - DateUtils.HOUR_TO_MILLIS,
                                                               validPollEpochRange.toEpoch + DateUtils.HOUR_TO_MILLIS)
            scheduledStoreIds.clear()

            for store in stores:
                dayUpDownHours = calculateUpDownHours(filteredEpochRangesDict[store.storeId], sortedPolledStats[store.storeId], store.onBoardedAt, pivotUtcEpoch)
                if store.storeId not in upDownHoursDict:
                    upDownHoursDict[store.storeId] = dayUpDownHours
                else:
                    prvUpDownHours = upDownHoursDict[store.storeId]
                    upDownHoursDict[store.storeId] = (prvUpDownHours[0] + dayUpDownHours[0], prvUpDownHours[1] + dayUpDownHours[1])
        return upDownHoursDict

    
    @staticmethod
    def getCsvText(storeReport:StoreReport):
        upTimeTextPart = f'{round(storeReport.uptime_last_hour * 60)},{"{.2f}".format(storeReport.uptime_last_day)},{"{.2f}".format(storeReport.uptime_last_week)}'
        downTimeTextPart = f'{round(storeReport.downtime_last_hour * 60)},{"{.2f}".format(storeReport.downtime_last_day)},{"{.2f}".format(storeReport.downtime_last_week)}'
        
        return f'{storeReport.store_id},{upTimeTextPart},{downTimeTextPart}'
    
    @staticmethod
    def getCsvHeader():
        upTimeHeaderPart = "uptime_last_hour (mins),uptime_last_day (hrs),uptime_last_week (hrs)"
        downTimeHeaderPart = "downtime_last_hour (mins),downtime_last_day (hrs),downtime_last_week (hrs)"

        return f'store_id,{upTimeHeaderPart},{downTimeHeaderPart}'