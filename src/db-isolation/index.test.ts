import _ from "lodash";
import {
  cleanUp,
  KeyValueTable,
  getKnex,
  runReadSkew,
  runWriteSkew,
  setup,
} from "./fixtures";

describe("db-isolation", () => {
  // docker exec -it db psql -U postgres
  beforeAll(async () => {
    await setup();
  }, 30_000);
  afterEach(async () => {
    await getKnex("pg")("key_value").truncate();
    await getKnex("mysql")("key_value").truncate();
  });
  afterAll(async () => {
    await getKnex("mysql").destroy();
    await getKnex("pg").destroy();
    await cleanUp();
  }, 10_000);

  describe("pg", () => {
    const dbType = "pg";
    const knex = getKnex(dbType);

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
      await expect(
        runWriteSkew({ dbType, isSerializable: true })
      ).rejects.toThrow(`Failed transaction`);
    });
  });

  describe("mysql", () => {
    const dbType = "mysql";
    const knex = getKnex(dbType);
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

    it("should not show read skew with default repeatable read (snapshot isolation)", async () => {
      const { firstRead, secondRead } = await runReadSkew({
        dbType,
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

    it("should error on write skew with serializable but actually does write skew", async () => {
      // This should throw
      const { result } = await runWriteSkew({ dbType, isSerializable: true });

      expect(result).toEqual([
        { key: "alice", value: "offcall" },
        { key: "bob", value: "offcall" },
      ]);
    });
  });
});
