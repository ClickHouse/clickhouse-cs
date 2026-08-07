-- Insert sink with a real MergeTree: sorting, compression, part writing, the lot.
--
-- The credibility arm. Truncate it between iterations (HitsInsertBenchmark does) — otherwise part
-- count grows across a run and later iterations pay merge costs the earlier ones did not, which reads
-- as a regression that is really just the table getting bigger.
--
-- The target database comes from the HTTP `database=` parameter (see 04-sink-null.sql).

CREATE TABLE IF NOT EXISTS hits_sink_mt AS hits ENGINE = MergeTree ORDER BY (CounterID, EventDate, UserID, EventTime, WatchID);
