import { runIncrement, runReadSkew, runWriteSkew } from "./fixtures";
import { setupDb, getKnex, cleanUpDb, KeyValueTable } from "./fixtures-setup";

describe("db-isolation/mysql", () => {
  const dbType = "mysql";
  before(async function () {
    this.timeout(30_000);
    await setupDb(dbType);
  });
  afterEach(async () => {
    await getKnex(dbType)("key_value").truncate();
  });
  after(async function () {
    this.timeout(10_000);
    await getKnex(dbType).destroy();
    await cleanUpDb(dbType);
  });

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
    ).to.deep.equal([input]);
  });

  it("should not show read skew with default repeatable read (snapshot isolation)", async () => {
    const { firstRead, secondRead } = await runReadSkew({
      dbType,
    });

    expect(firstRead).to.deep.equal("my-value1");
    expect(secondRead).to.deep.equal("my-value1");
  });

  it("should demonstrate write skew", async () => {
    const { result } = await runWriteSkew({ dbType });

    expect(result).to.deep.equal([
      { key: "alice", value: "offcall" },
      { key: "bob", value: "offcall" },
    ]);
  });

  it("should error on write skew with serializable but actually does write skew", async () => {
    // This throws in postgres, but not in mysql
    const { result } = await runWriteSkew({ dbType, isSerializable: true });

    expect(result).to.deep.equal([
      { key: "alice", value: "offcall" },
      { key: "bob", value: "offcall" },
    ]);
  });

  it("should not error for increment in mysql for writing to stale read row (last write wins)", async () => {
    const { result } = await runIncrement({ dbType });
    expect(result).to.deep.equal(1);
  });
});
