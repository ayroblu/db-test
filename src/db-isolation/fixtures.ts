import { DbType, getKnex, KeyValueTable } from "./fixtures-setup";

type ReadSkewOptions = {
  isRepeatableRead?: boolean;
  dbType: "pg" | "mysql";
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

  const { trx, trx2 } = await getTwoTransactions({
    dbType,
    isolationLevel: isRepeatableRead ? "repeatable read" : "default",
  });
  await trx<KeyValueTable>("key_value").update({ value: "my-value2" }).where({ key: "my-key1" });
  await trx<KeyValueTable>("key_value").update({ value: "my-value2" }).where({ key: "my-key2" });

  const firstRead = await trx2<KeyValueTable>("key_value")
    .select("value")
    .where({ key: "my-key1" })
    .then((a) => a[0].value);
  await trx.commit();
  const secondRead = await trx2<KeyValueTable>("key_value")
    .select("value")
    .where({ key: "my-key2" })
    .then((a) => a[0].value);
  await trx2.commit();

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
async function getTwoTransactions({ dbType, isolationLevel }: TransactionOptions) {
  const knex = getKnex(dbType);
  if (dbType === "mysql") {
    if (isolationLevel === "default" || isolationLevel === "repeatable read") {
      await knex.raw("SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
    } else if (isolationLevel === "serializable") {
      await knex.raw("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
    }
  }

  const trx = await knex.transaction();
  const trx2 = await knex.transaction();
  try {
    if (dbType === "pg" && isolationLevel === "repeatable read") {
      await trx.raw("set transaction isolation level repeatable read;");
      await trx2.raw("set transaction isolation level repeatable read;");
    } else if (dbType === "pg" && isolationLevel === "serializable") {
      await trx.raw("set transaction isolation level serializable;");
      await trx2.raw("set transaction isolation level serializable;");
    }
  } catch (err) {
    await Promise.all([trx.rollback(), trx2.rollback()]);
    throw err;
  }
  return {
    trx,
    trx2,
  };
}
