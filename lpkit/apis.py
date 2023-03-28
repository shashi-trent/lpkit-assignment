from uuid import uuid4
from lpkit.classes import ReportStatus
from lpkit.models import ReportStat
from lpkit.repos import ReportRepo
from lpkit.services import ReportService
from lpkit.taskexecutors import ReportExecutor, runInBg
from lpkit.utils import DateUtils


class ReportApis:
    @staticmethod
    def get(reportId:str):
        reportStat = ReportRepo.getStat(reportId)
        if reportStat is None:
            return {"ok": False, "msg": "reportId do not exist"}
        else:
            res = {"ok": True, "status": reportStat.status.name, "version": reportStat.version,
                    "startedAt": reportStat.runAt, "id": reportStat.id,
                    "timeTaken": DateUtils.toIsoFormat(DateUtils.curUtcEpoch() - reportStat.runAt),
                    "filename": None
                }
            if reportStat.status == ReportStatus.Complete:
                res.update({
                    "timeTaken": DateUtils.toIsoFormat(reportStat.completedAt - reportStat.runAt),
                    "filename": f"report-{reportStat.id}-v{reportStat.version}"
                })
            return res

    @staticmethod
    def generate(pivotUtcEpoch:int|None):
        reportStat = ReportStat(row={
            ReportStat._Id: str(uuid4()),
            ReportStat._RunAt: DateUtils.curUtcEpoch(),
            ReportStat._Version: 0,
            ReportStat._Status: ReportStatus.Running.value
        })
        
        ReportRepo.saveStat(reportStat)
        runInBg(ReportExecutor, ReportService.generate, reportStat, pivotUtcEpoch or reportStat.runAt)
        return {"ok": True, "version": reportStat.version,
                "startedAt": reportStat.runAt, "id": reportStat.id,
                "status": reportStat.status.name, "timeTaken": None, "filename": None
            }
