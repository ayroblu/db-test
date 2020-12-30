import { runReadSkewMssql, runWriteSkewMssql } from "./fixtures";
import { cleanUpDb, getKnex, KeyValueTable, setupDb } from "./fixtures-setup";
import { execSync } from "child_process";

describe("db-isolation/mssql", () => {
  // docker exec -it <container_id|container_name> /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P <your_password>
  const dbType = "mssql";
  beforeAll(async () => {
    await setupDb(dbType);
  }, 30_000);
  afterEach(async () => {
    await getKnex(dbType)("key_value").truncate();
  });
  afterAll(async () => {
    execSync("docker logs mssql", { stdio: "inherit" });
    await getKnex(dbType).destroy();
    await cleanUpDb(dbType);
  }, 10_000);

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
    const { firstRead, secondRead } = await runReadSkewMssql("repeatable read");

    expect(firstRead).not.toEqual(secondRead);
  });

  it("should eliminate read skew with repeatable read (snapshot isolation)", async () => {
    const { firstRead, secondRead } = await runReadSkewMssql("snapshot");

    expect(firstRead).toEqual(secondRead);
  });

  it("should demonstrate write skew", async () => {
    const { result } = await runWriteSkewMssql("snapshot");

    expect(result).toEqual([
      { key: "alice", value: "offcall" },
      { key: "bob", value: "offcall" },
    ]);
  });

  it("should error on write skew with serializable for deadlock - killed", async () => {
    await expect(runWriteSkewMssql("serializable")).rejects.toThrow(
      `Cannot continue the execution because the session is in the kill state`
    );
  });
});
