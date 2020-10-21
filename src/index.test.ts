import { getDb } from "./setup-tests";
import fs from "fs";
import path from "path";

const createKvTableSql = fs.readFileSync(
  path.join(__dirname, "./sql/create-kv-table.sql"),
  "utf-8"
);

describe("No Setup", () => {
  const db = getDb();

  afterAll(async () => db.then(({ teardown }) => teardown()));

  test("inserting rows", async () => {
    const { knex } = await db;
    const hrstart = process.hrtime();
    await knex.raw(createKvTableSql);
    const hrend = process.hrtime(hrstart);
    await knex("kv").insert({ key: "123", value: "345" });
    const hrend2 = process.hrtime(hrstart);
    console.log(hrend, hrend2);
    // todo
  });
});
