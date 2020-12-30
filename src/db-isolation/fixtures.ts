import { DbType, getKnex, KeyValueTable, wait } from "./fixtures-setup";
import mssql from "mssql";

type ReadSkewOptions = {
  isRepeatableRead?: boolean;
  dbType: DbType;
};
/**
 * Reading before and after a transaction has commited violates an application invariant
 */
export async function runReadSkew({ isRepeatableRead, dbType }: ReadSkewOptions) {
  const knex = getKnex(dbType);
  const input = [
    {
      key: "my-key1",
      value: "my-value1",
    },
    {
      key: "my-key2",
      value: "my-value1",
    },
  ];
  await knex<KeyValueTable>("key_value").insert(input);

  const trx = await getTransaction({
    dbType,
    isolationLevel: isRepeatableRead ? "repeatable read" : "default",
  });

  const firstRead = await trx<KeyValueTable>("key_value")
    .select("value")
    .where({ key: "my-key1" })
    .then((a) => a[0].value);
  await knex<KeyValueTable>("key_value")
    .update({ value: "my-value2" })
    .whereIn("key", ["my-key1", "my-key2"]);
  const secondRead = await trx<KeyValueTable>("key_value")
    .select("value")
    .where({ key: "my-key2" })
    .then((a) => a[0].value);
  await trx.commit();

  return { firstRead, secondRead };
}

type WriteSkewOptions = {
  isSerializable?: boolean;
  dbType: DbType;
};
/**
 * Parallel writes on an item that depends on a read violate an application invariant
 */
export async function runWriteSkew({ isSerializable, dbType }: WriteSkewOptions) {
  const knex = getKnex(dbType);
  const input = [
    { key: "alice", value: "oncall" },
    { key: "bob", value: "oncall" },
  ];
  await knex<KeyValueTable>("key_value").insert(input);
  const { trx, trx2 } = await getTwoTransactions({
    dbType,
    isolationLevel: isSerializable ? "serializable" : "repeatable read",
  });

  try {
    const oncalls = await trx<KeyValueTable>("key_value").select("value").where("value", "oncall");
    const oncallsBob = await trx2<KeyValueTable>("key_value")
      .select("value")
      .where("value", "oncall");
    if (oncalls.length > 1) {
      await trx<KeyValueTable>("key_value").update({ value: "offcall" }).where({ key: "alice" });
    }
    await trx.commit();
    if (oncallsBob.length > 1) {
      await trx2<KeyValueTable>("key_value").update({ value: "offcall" }).where({ key: "bob" });
    }
  } catch (err) {
    await Promise.all([trx.rollback(), trx2.rollback()]);
  }
  const transactionResult = await trx2.commit();
  if (!transactionResult) {
    throw new Error("Failed transaction");
  }

  const result = await knex<KeyValueTable>("key_value")
    .select("key", "value")
    .orderBy("key")
    .then((res) => res.map(({ key, value }) => ({ key, value })));

  return { result };
}

type IncrementOptions = {
  dbType: DbType;
};
/**
 * Parallel writes on an item that depends on a read violate an application invariant
 */
export async function runIncrement({ dbType }: IncrementOptions) {
  const knex = getKnex(dbType);
  const input = [{ key: "my-key", value: "0" }];
  await knex<KeyValueTable>("key_value").insert(input);
  const { trx, trx2 } = await getTwoTransactions({ dbType, isolationLevel: "repeatable read" });
  try {
    const value1 = await trx<KeyValueTable>("key_value")
      .select("value")
      .where("key", "my-key")
      .then(([{ value }]) => parseInt(value, 10));
    const value2 = await trx2<KeyValueTable>("key_value")
      .select("value")
      .where("key", "my-key")
      .then(([{ value }]) => parseInt(value, 10));
    await trx<KeyValueTable>("key_value")
      .update({ value: value1 + 1 + "" })
      .where({ key: "my-key" });
    await trx.commit();
    await trx2<KeyValueTable>("key_value")
      .update({ value: value2 + 1 + "" })
      .where({ key: "my-key" });
    await trx2.commit();
  } catch (err) {
    await Promise.all([trx.rollback(), trx2.rollback()]);
    throw err;
  }

  const result = await knex<KeyValueTable>("key_value")
    .select("key", "value")
    .where({ key: "my-key" })
    .then(([{ value }]) => parseInt(value, 10));

  return { result };
}

type TransactionOptions = {
  dbType: DbType;
  isolationLevel: "default" | "repeatable read" | "serializable";
};
async function getTwoTransactions(options: TransactionOptions) {
  const [trx, trx2] = await Promise.all([getTransaction(options), getTransaction(options)]);
  return {
    trx,
    trx2,
  };
}

async function getTransaction({ dbType, isolationLevel }: TransactionOptions) {
  const knex = getKnex(dbType);
  if (dbType === "mysql") {
    if (isolationLevel === "default" || isolationLevel === "repeatable read") {
      await knex.raw("SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
    } else if (isolationLevel === "serializable") {
      await knex.raw("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
    }
  }
  if (dbType === "mssql") {
    if (isolationLevel === "repeatable read") {
      await knex.raw("SET TRANSACTION ISOLATION LEVEL SNAPSHOT");
    } else if (isolationLevel === "serializable") {
      await knex.raw("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    }
  }

  const trx = await knex.transaction();
  try {
    if (dbType === "pg") {
      if (isolationLevel === "repeatable read") {
        await trx.raw("set transaction isolation level repeatable read;");
      } else if (isolationLevel === "serializable") {
        await trx.raw("set transaction isolation level serializable;");
      }
    }
  } catch (err) {
    await trx.rollback();
    throw err;
  }
  return trx;
}

export async function runReadSkewMssql(isolationLevel: "repeatable read" | "snapshot") {
  const knex = getKnex("mssql");
  const input = [
    {
      key: "my-key1",
      value: "my-value1",
    },
  ];
  await knex<KeyValueTable>("key_value").insert(input);

  const mssqlIsolationLevel =
    isolationLevel === "repeatable read"
      ? mssql.ISOLATION_LEVEL.REPEATABLE_READ
      : mssql.ISOLATION_LEVEL.SNAPSHOT;

  const config = {
    server: "localhost",
    user: "sa",
    password: "yourStrong(!)Password",
    database: "mydb",
    requestTimeout: 1000,
    "request timeout": 1000,
  };
  const pool = new mssql.ConnectionPool(config);
  await pool.connect();
  try {
    const trx = new mssql.Transaction(pool);
    await trx.begin(mssqlIsolationLevel);
    const { recordset: firstRead } = await new mssql.Request(trx).query(
      knex<KeyValueTable>("key_value").select().toString()
    );

    await knex<KeyValueTable>("key_value").insert({ key: "my-key2", value: "my-value2" });
    const { recordset: secondRead } = await new mssql.Request(trx).query(
      knex<KeyValueTable>("key_value").select().toString()
    );
    await trx.commit();

    return { firstRead, secondRead };
  } finally {
    await pool.close();
  }
}

export async function runWriteSkewMssql(isolationLevel: "snapshot" | "serializable") {
  const knex = getKnex("mssql");

  const input = [
    { key: "alice", value: "oncall" },
    { key: "bob", value: "oncall" },
  ];
  await knex<KeyValueTable>("key_value").insert(input);
  // 1 ms
  await knex.raw("SET LOCK_TIMEOUT 1");

  const mssqlIsolationLevel =
    isolationLevel === "snapshot"
      ? mssql.ISOLATION_LEVEL.SNAPSHOT
      : mssql.ISOLATION_LEVEL.SERIALIZABLE;

  const config = {
    server: "localhost",
    user: "sa",
    password: "yourStrong(!)Password",
    database: "mydb",
    requestTimeout: 1000,
    connectionTimeout: 1000,
  };
  const pool = new mssql.ConnectionPool(config);
  await pool.connect();
  try {
    const trx1 = new mssql.Transaction(pool);
    const trx2 = new mssql.Transaction(pool);
    await trx1.begin(mssqlIsolationLevel);
    await trx2.begin(mssqlIsolationLevel);
    const oncallsAlice = await new mssql.Request(trx1).query(
      knex<KeyValueTable>("key_value").select("value").where("value", "oncall").toString()
    );
    const oncallsBob = await new mssql.Request(trx2).query(
      knex<KeyValueTable>("key_value").select("value").where("value", "oncall").toString()
    );
    const hasEnoughOncallAlice = oncallsAlice.recordset.length > 1;
    const hasEnoughOncallBob = oncallsBob.recordset.length > 1;
    async function goOffcall(name: string, hasEnoughOncall: boolean, trx: mssql.Transaction) {
      if (hasEnoughOncall) {
        await new mssql.Request(trx).query(
          knex<KeyValueTable>("key_value")
            .update({ value: "offcall" })
            .where({ key: name })
            .toString()
        );
      }
      return trx.commit();
    }
    await mssqlLockKiller(() =>
      Promise.all([
        goOffcall("alice", hasEnoughOncallAlice, trx1),
        goOffcall("bob", hasEnoughOncallBob, trx2),
      ])
    );
  } finally {
    await pool.close();
  }

  const result = await knex<KeyValueTable>("key_value")
    .select("key", "value")
    .orderBy("key")
    .then((res) => res.map(({ key, value }) => ({ key, value })));

  return { result };
}
async function mssqlLockKiller<T>(func: () => Promise<T>): Promise<T> {
  const knex = getKnex("mssql");
  return Promise.all([
    wait(10)
      .then(() => knex.raw(`select cmd,* from sys.sysprocesses where blocked > 0`))
      .then((result) =>
        Promise.all(result.map(({ spid }: { spid: number }) => knex.raw(`kill ${spid}`)))
      ),
    func(),
  ]).then(([_, r2]) => r2);
}
