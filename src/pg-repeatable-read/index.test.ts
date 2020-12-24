import _ from "lodash";
import {
  cleanUp,
  KeyValueTable,
  knex,
  runReadSkew,
  runWriteSkew,
  setup,
} from "./fixtures";

describe("pg-repeatable-read", () => {
  // docker exec -it db psql -U postgres
  beforeAll(async () => {
    await setup();
  });
  afterEach(async () => {
    await knex("key_value").truncate();
  });
  afterAll(async () => {
    await cleanUp();
    await knex.destroy();
  });

  it("should be able to interpret an insert and read", async () => {
    const input = {
      key: "my-key",
      value: "my-value",
    };
    await knex<KeyValueTable>("key_value").insert(input);
    const selectResult = await knex<KeyValueTable>("key_value")
      .select()
      .where("key", input.key);
    expect(
      selectResult.map(({ key, value }) => ({
        key,
        value,
      }))
    ).toEqual([input]);
  });

  it("should demonstrate read skew", async () => {
    const { firstRead, secondRead } = await runReadSkew();

    expect(firstRead).toEqual("my-value1");
    expect(secondRead).toEqual("my-value2");
  });

  it("should eliminate read skew with repeatable read (snapshot isolation)", async () => {
    const { firstRead, secondRead } = await runReadSkew({
      isRepeatableRead: true,
    });

    expect(firstRead).toEqual("my-value1");
    expect(secondRead).toEqual("my-value1");
  });

  it("should demonstrate write skew", async () => {
    const { result } = await runWriteSkew();

    expect(result).toEqual("1");
  });

  it("should error on write skew with serializable", async () => {
    await expect(runWriteSkew({ isSerializable: true })).rejects.toThrow(
      `update "key_value" set "value" = $1 where "key" = $2 - could not serialize access due to concurrent update`
    );
  });
});
