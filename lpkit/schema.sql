DROP TABLE IF EXISTS store_poll_stat;
DROP TABLE IF EXISTS store_schedule;
DROP TABLE IF EXISTS store_info;
DROP TABLE IF EXISTS report_stat;

CREATE TABLE store_poll_stat (
  store_id BIGINT NOT NULL,
  `status` VARCHAR(32) NOT NULL,
  timestamp_utc DATETIME NOT NULL,

  PRIMARY KEY(store_id, timestamp_utc)
);

CREATE TABLE store_schedule (
  store_id BIGINT NOT NULL,
  dayOfWeek INTEGER NOT NULL,
  start_time_local TIME NOT NULL DEFAULT '00:00:00',
  end_time_local TIME NOT NULL DEFAULT '23:59:59.9999'
);
CREATE INDEX idx_id_day_storesch ON store_schedule (store_id, dayOfWeek);

CREATE TABLE store_info (
  store_id BIGINT NOT NULL,
  timezone_str VARCHAR(64),

  PRIMARY KEY(store_id)
);

CREATE TABLE report_stat (
  id VARCHAR(64) NOT NULL PRIMARY KEY,
  `version` INTEGER,
  `status` INTEGER NOT NULL,
  run_at BIGINT,
  completed_at BIGINT
);