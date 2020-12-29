import { runIncrement, runReadSkew, runWriteSkew } from "./fixtures";
import { setupDb, getKnex, cleanUpDb, KeyValueTable } from "./fixtures-setup";

describe("db-isolation/pg", () => {
  // docker exec -it db psql -U postgres
  beforeAll(async () => {
    await setupDb("pg");
  }, 30_000);
  afterEach(async () => {
    await getKnex("pg")("key_value").truncate();
  });
  afterAll(async () => {
    await getKnex("pg").destroy();
    await cleanUpDb("pg");
  }, 10_000);

  const dbType = "pg";
  const knex = getKnex(dbType);

  it("should be able to interpret an insert and read", async () => {
    const input = {
      key: "my-key",
      value: "my-value",
    };
    await knex<KeyValueTable>("key_value").insert(input);
    const selectResult = await knex<KeyValueTable>("key_value").select().where("key", input.key);
    expect(
      selectResult.map(({ key, value }) => ({
        key,
        value,
      }))
    ).toEqual([input]);
  });

  it("should demonstrate read skew", async () => {
    const { firstRead, secondRead } = await runReadSkew({ dbType });

    expect(firstRead).toEqual("my-value1");
    expect(secondRead).toEqual("my-value2");
  });

  it("should eliminate read skew with repeatable read (snapshot isolation)", async () => {
    const { firstRead, secondRead } = await runReadSkew({
      dbType,
      isRepeatableRead: true,
    });

    expect(firstRead).toEqual("my-value1");
    expect(secondRead).toEqual("my-value1");
  });

  it("should demonstrate write skew", async () => {
    const { result } = await runWriteSkew({ dbType });

    expect(result).toEqual([
      { key: "alice", value: "offcall" },
      { key: "bob", value: "offcall" },
    ]);
  });

  it("should error on write skew with serializable for missing commit", async () => {
    await expect(runWriteSkew({ dbType, isSerializable: true })).rejects.toThrow(
      `Failed transaction`
    );
  });

  it("should error for increment in postgres for writing to stale read row", async () => {
    await expect(runIncrement({ dbType })).rejects.toThrow(
      'update "key_value" set "value" = $1 where "key" = $2 - could not serialize access due to concurrent update'
    );
  });
});
