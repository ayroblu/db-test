CREATE TABLE key_value (
  `key` VARCHAR(100) NOT NULL,
  value TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
  CONSTRAINT key_pk PRIMARY KEY (`key`)
);
