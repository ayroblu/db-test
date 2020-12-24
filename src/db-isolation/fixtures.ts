import { execSync } from "child_process";
import knexBuilder from "knex";

const pgImageName = "db-tests/pg-isolation";
const mysqlImageName = "db-tests/mysql-isolation";
const password = "mysecretpassword";

const pgKnex = knexBuilder({
  client: "pg",
  connection: {
    host: "127.0.0.1",
    user: "postgres",
    password,
    database: "postgres",
  },
});
const mysqlKnex = knexBuilder({
  client: "mysql",
  connection: {
    host: "127.0.0.1",
    user: "root",
    password,
    database: "mydb",
  },
});
type DbType = "pg" | "mysql";
export const getKnex = (dbType: DbType) => {
  if (dbType === "pg") {
    return pgKnex;
  } else {
    return mysqlKnex;
  }
};
const getImageName = (dbType: DbType) => {
  if (dbType === "pg") {
    return pgImageName;
  } else {
    return mysqlImageName;
  }
};

export type KeyValueTable = {
  id: string;
  key: string;
  value: string;
  created_at: Date;
};

export const cleanUp = async () => {
  await Promise.all([cleanUpDb("pg"), cleanUpDb("mysql")]);
};
const cleanUpDb = async (dbType: DbType) => {
  const stdout = execSync(`set -x; docker ps -aqf "name=${dbType}"`, {
    encoding: "utf-8",
  });
  const containerId = stdout.trim();
  if (containerId) {
    execSync(
      `set -x; docker stop "${containerId}" && docker rm "${containerId}"`,
      {
        stdio: "inherit",
      }
    );
  }
};
export const setup = async () => {
  await Promise.all([setupDb("pg"), setupDb("mysql")]);
};
const setupDb = async (dbType: DbType) => {
  const knex = getKnex(dbType);
  await cleanUpDb(dbType);
  const imageName = getImageName(dbType);
  execSync(
    `set -x; docker build -t ${imageName} -f ${__dirname}/Dockerfile-${dbType} ${__dirname}`,
    {
      stdio: "inherit",
    }
  );
  if (dbType === "pg") {
    execSync(
      `set -x; docker run -p 5432:5432 --name ${dbType} -e POSTGRES_PASSWORD=${password} -d ${imageName}:latest`,
      { stdio: "inherit" }
    );
  } else {
    execSync(
      `set -x; docker run -p 3306:3306 --name ${dbType} -e MYSQL_DATABASE=mydb -e MYSQL_ROOT_PASSWORD=${password} -d ${imageName}:latest`,
      { stdio: "inherit" }
    );
  }
  await new Promise(async (resolve) => {
    while (true) {
      try {
        await knex.select(knex.raw("1"));
        resolve();
        return;
      } catch (err) {
        // console.log("Waiting because:", err.message);
        await wait(1000);
      }
    }
  });
  return dbType;
};
const wait = (t: number) => new Promise((y) => setTimeout(y, t));

type ReadSkewOptions = {
  isRepeatableRead?: boolean;
  dbType: "pg" | "mysql";
};
/**
 * Reading before and after a transaction has commited violates an application invariant
 */
export async function runReadSkew({
  isRepeatableRead,
  dbType,
}: ReadSkewOptions) {
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

  const trx = await knex.transaction();
  const trx2 = await knex.transaction();
  if (dbType === "pg" && isRepeatableRead) {
    await trx.raw("set transaction isolation level repeatable read;");
    await trx2.raw("set transaction isolation level repeatable read;");
  }
  await trx<KeyValueTable>("key_value")
    .update({ value: "my-value2" })
    .where({ key: "my-key1" });
  await trx<KeyValueTable>("key_value")
    .update({ value: "my-value2" })
    .where({ key: "my-key2" });

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
export async function runWriteSkew({
  isSerializable,
  dbType,
}: WriteSkewOptions) {
  const knex = getKnex(dbType);
  const input = [
    { key: "alice", value: "oncall" },
    { key: "bob", value: "oncall" },
  ];
  await knex<KeyValueTable>("key_value").insert(input);
  if (dbType === "mysql" && isSerializable) {
    await knex.raw("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
  } else if (dbType === "mysql" && !isSerializable) {
    await knex.raw("SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
  }

  const trx = await knex.transaction();
  const trx2 = await knex.transaction();
  if (dbType === "pg") {
    if (isSerializable) {
      await trx.raw("set transaction isolation level serializable;");
      await trx2.raw("set transaction isolation level serializable;");
    } else {
      await trx.raw("set transaction isolation level repeatable read;");
      await trx2.raw("set transaction isolation level repeatable read;");
    }
  }
  const oncalls = await trx<KeyValueTable>("key_value")
    .select("value")
    .where("value", "oncall");
  const oncallsBob = await trx2<KeyValueTable>("key_value")
    .select("value")
    .where("value", "oncall");
  if (oncalls.length > 1) {
    await trx<KeyValueTable>("key_value")
      .update({ value: "offcall" })
      .where({ key: "alice" });
  }
  if (oncallsBob.length > 1) {
    await trx2<KeyValueTable>("key_value")
      .update({ value: "offcall" })
      .where({ key: "bob" });
  }
  await trx.commit();
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
