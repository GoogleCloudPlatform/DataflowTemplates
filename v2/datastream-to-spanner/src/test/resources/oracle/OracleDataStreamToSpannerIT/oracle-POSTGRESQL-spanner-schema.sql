CREATE TABLE IF NOT EXISTS "DatastreamToSpanner_1" (
  row_id bigint NOT NULL,
  name character varying,
  age double precision,
  member character varying,
  entry_added character varying,
  PRIMARY KEY (row_id)
);

CREATE TABLE IF NOT EXISTS "DatastreamToSpanner_2" (
  row_id bigint NOT NULL,
  name character varying,
  age double precision,
  member character varying,
  entry_added character varying,
  PRIMARY KEY (row_id)
);
