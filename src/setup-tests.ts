import Knex from "knex";
import { setupDb, teardownDb } from "./setup-db";

let _knex: { knex: Knex; teardown: () => Promise<void> } | null = null;

export async function getDb() {
  if (_knex) {
    return _knex;
  }
  const { dbName, knex } = await setupDb();
  return (_knex = {
    knex,
    teardown: () => teardownDb(dbName),
  });
}
export async function resetDb() {
  const { dbName, knex } = await setupDb();
  return (_knex = {
    knex,
    teardown: () => teardownDb(dbName),
  });
}
