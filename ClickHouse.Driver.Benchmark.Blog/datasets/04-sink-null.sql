-- Insert sink with ENGINE = Null: the server parses the RowBinary block and discards it.
--
-- This is the arm for every client-cost claim — serialization, compression, transport — because it
-- removes storage entirely. SAY SO next to any number from it: it is not what an insert into a real
-- table costs.
--
-- Schema is cloned from bench.hits with `AS`, so the 105-column list has exactly one definition and
-- the sink cannot silently drift from the source table.
--
-- The target database comes from the HTTP `database=` parameter: ClickHouse cannot parse
-- `CREATE TABLE x AS {db:Identifier}.y ENGINE = ...` — the parameterized identifier in the AS-source
-- position swallows the ENGINE clause (verified on 25.10.7.6).

CREATE TABLE IF NOT EXISTS hits_sink_null AS hits ENGINE = Null;
