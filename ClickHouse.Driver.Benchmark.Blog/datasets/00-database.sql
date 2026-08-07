-- Creates the benchmark database.
--
-- Split out so every other file in this directory is exactly ONE statement: the HTTP interface takes
-- one statement per request, and splitting a multi-statement file on ';' in a shell script breaks the
-- moment a literal contains one.
--
-- Parameters: {db:Identifier}

CREATE DATABASE IF NOT EXISTS {db:Identifier};
