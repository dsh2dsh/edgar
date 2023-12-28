-- -*- sql-product: postgres; -*-

DROP TABLE IF EXISTS companies CASCADE;
CREATE TABLE companies (
  cik         INTEGER PRIMARY KEY,
  entity_name TEXT    NOT NULL
);

DROP TABLE IF EXISTS facts CASCADE;
CREATE TABLE facts (
  id        SERIAL PRIMARY KEY,
  fact_tax  TEXT   NOT NULL,
  fact_name TEXT   NOT NULL,
  UNIQUE (fact_tax, fact_name)
);

DROP TABLE IF EXISTS fact_labels;
CREATE TABLE fact_labels (
  id         SERIAL  PRIMARY KEY,
  fact_id    INTEGER NOT NULL REFERENCES facts(id),
  fact_label TEXT    NOT NULL,
  descr      TEXT    NOT NULL,
  xxhash1    NUMERIC NOT NULL,
  xxhash2    NUMERIC NOT NULL,
  UNIQUE(fact_id, xxhash1, xxhash2)
);

DROP TABLE IF EXISTS units CASCADE;
CREATE TABLE units (
  id        SERIAL PRIMARY KEY,
  unit_name TEXT   NOT NULL UNIQUE
);

DROP TABLE IF EXISTS fact_units;
CREATE TABLE fact_units (
  company_cik INTEGER NOT NULL REFERENCES companies(cik),
  fact_id     INTEGER NOT NULL REFERENCES facts(id),
  unit_id     INTEGER NOT NULL REFERENCES units(id),
  fact_start  DATE,
  fact_end    DATE    NOT NULL,
  val         NUMERIC NOT NULL,
  accn        TEXT    NOT NULL,
  fy          INTEGER NOT NULL,
  fp          TEXT    NOT NULL,
  form        TEXT    NOT NULL,
  filed       DATE    NOT NULL,
  frame       TEXT,
  PRIMARY KEY (company_cik, fact_id, unit_id)
);
