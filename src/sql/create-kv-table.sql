CREATE TABLE kv (
  -- id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
)
