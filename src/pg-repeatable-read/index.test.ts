import _ from "lodash";
import { cleanUp, knex, setup } from "./fixtures";

interface KeyValueTable {
  id: string;
  key: string;
  value: string;
  created_at: Date;
}
describe("pg-repeatable-read", () => {
  beforeAll(async () => {
    await setup();
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
});
