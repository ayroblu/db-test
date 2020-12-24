CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE key_value (
  -- id uuid PRIMARY KEY DEFAULT uuid_generate_v4 (),
  key TEXT NOT NULL PRIMARY KEY,
  value TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
