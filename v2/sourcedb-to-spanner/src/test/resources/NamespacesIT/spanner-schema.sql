CREATE TABLE singers (
  singer_id INT64 NOT NULL,
  first_name STRING(1024)
) PRIMARY KEY (singer_id);

CREATE TABLE albums (
  singer_id INT64 NOT NULL,
  album_id INT64 NOT NULL,
  album_serial_number INT64,
  CONSTRAINT album_id_fk FOREIGN KEY (album_id) REFERENCES singers (singer_id)
) PRIMARY KEY (singer_id, album_id);

CREATE INDEX album_serial_number_idx ON albums (album_serial_number);
