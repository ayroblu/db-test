import { runIncrement, runReadSkew, runWriteSkew } from "./fixtures";
import { setupDb, getKnex, cleanUpDb, KeyValueTable } from "./fixtures-setup";

describe("db-isolation/pg", () => {
  // docker exec -it db psql -U postgres
  const dbType = "pg";
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

  it("should demonstrate read skew", async () => {
    const { firstRead, secondRead } = await runReadSkew({ dbType });

    expect(firstRead).to.deep.equal("my-value1");
    expect(secondRead).to.deep.equal("my-value2");
  });

  it("should eliminate read skew with repeatable read (snapshot isolation)", async () => {
    const { firstRead, secondRead } = await runReadSkew({
      dbType,
      isRepeatableRead: true,
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

  it("should error on write skew with serializable for missing commit", async () => {
    await expect(runWriteSkew({ dbType, isSerializable: true })).to.be.rejectedWith(
      `Failed transaction`
    );
  });

  it("should error for increment in postgres for writing to stale read row", async () => {
    await expect(runIncrement({ dbType })).to.be.rejectedWith(
      'update "key_value" set "value" = $1 where "key" = $2 - could not serialize access due to concurrent update'
    );
  });
});
