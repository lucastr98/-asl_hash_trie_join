COPY r FROM '/tmp/synthetic-data/R.csv' WITH (FORMAT csv, DELIMITER '|');
COPY s FROM '/tmp/synthetic-data/S.csv' WITH (FORMAT csv, DELIMITER '|');
COPY t FROM '/tmp/synthetic-data/T.csv' WITH (FORMAT csv, DELIMITER '|');
