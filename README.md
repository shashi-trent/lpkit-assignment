# LoopKitchen Assignment

this app opens a web-page which has good navigation to report-generation-flow
<br><br>

## requirements
`Flask` and `pytz`
<br><br>

## terminology (briefly)

`pivot epoch time (tz=utc)` this is epoch time (in millis) which lets you alter the report-time-perspective. basically acts as if you have changed the current-time picker of the system which populates timings of last{Hour/Day/Week}

`picking last {hour / day / week}` say pivotTime = 1611829080000 (Thu, 28 Jan 2021 10:18:00 TZ),
<br>

then `lastHour`=(sameDate 09:00:00 TZ  to  10:00:00 TZ)
<br>

then `lastDay`=(Wed, 27 Jan 2021 00:00:00 TZ  to  23:59:59.9999 TZ)
<br>

then `lastWeek`=(Mon, 18 Jan 2021 00:00:00 TZ  to Sun, 24 Jan  23:59:59.9999 TZ)
<br><br>

`ignoreTill : epoch time (tz=utc)` a store-related-variable used inside methods to specify from when this store is with us (or its professional-onboarding-time) so prior to this, any garbage polled-status values will not be used.

`store_info` db table for store_&_timezone-str csv values, this table is used as primary store_id keeper, so all store_id must be present in this table (timezone_str can be null, code will use default in that case).

`store_poll_status` db table for polled_store_status-data, this table has primary=(store_id, timestamp_utc), cause assignee did not find any relevance to have multiple entry of same-store on same-time. so duplicates inside csv must be managed while importing to db. 

`STORE_TZS_DBFETCH_BATCH_SIZE : int` fetching store-timezones by timezone sorted manner with this int as batch-size.

`POLL_STAT_DBFETCH_BATCH_SIZE : int` batchSize for fetching polled-status data at a time.
<br><br>


## logic (briefly)

1. fetched timezone_str sorted store_infos batch-wise
2. on each batch, run to get separate last{hour/day/week} sub-reports
3. for each subReport, break down above batches in smaller batches to fetch polled-store_status, while keeping these smaller batches timezone-coherent (every store_info in a particular smaller batch have same timezone)
4. calculate subReport (last{Hour/day/week}) timings as explained earlier, and map polled-statuses, with store-schedules within that subReport-Timing-range
> subReport-timing-range have extra-1hr wrapped around the actual timing, to ensure extrapolation from those extra polled-store_statuses

<br>

## extrapolation (briefly)

say we have 2 polled-statuses (`9:10:00-active`, and `10:56:00-inactive`)
<br>

find the middle of these = `10:03:00`
<br>

we mark 1st-half time-slot `(9:10:00 - 10:03:00)=active`,

and 2nd-half time-slot `(10:03:00 - 10:56:00)=inactive`

> to extrapolate to both infinity, code assumes one polled-status at utcEpoch=0, and other at utcEpoch=pivotTime,  with both having status=None. having None as status means its half-slot will not belong to any of these active or inactive; and hence will not be added to either of those also.

<br>

## run
`flask --app lpkit init-db`
to initialize database with tables-schema in lpkit/schema.sql
<br><br>

`flask --app lpkit run --debug`
to run app in debug mode

<br><br>

## layout

<pre>
lpkit-assignment
│ 
├── lpkit/
│   ├── __init__.py      : main app 
│   ├── config.py        : app config values
│   ├── schema.sql       : db schema
│   ├── db.py            : db connection
│   ├── models.py        : db models
│   ├── repos.py         : db-queries
│   ├── services.py      : business-logic
│   ├── apis.py          : report-api-router
│   ├── taskexecutors.py : async-pool-executor
│   ├── utils.py         : utilities
│   ├── classes.py       : enums & data-classes
│   ├── configkeeper.py  : config-value-accessor
│   └── templates/
│       └── base.html    : for web-page view
│   
├── reportcsvs/          : find reports-csv here
└── instance/
    └── lpkit.sqlite     : created when init-db

</pre>

