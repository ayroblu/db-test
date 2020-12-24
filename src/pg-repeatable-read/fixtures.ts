import { exec, execSync } from "child_process";
import knexBuilder from "knex";
import { promisify } from "util";

const execPromise = promisify(exec);
const myImageName = "db-tests/pg-repeatable-read";
const myContainerName = "db";
const postgresPassword = "mysecretpassword";

export const knex = knexBuilder({
  client: "pg",
  connection: {
    host: "127.0.0.1",
    user: "postgres",
    password: postgresPassword,
    database: "postgres",
  },
});

export type KeyValueTable = {
  id: string;
  key: string;
  value: string;
  created_at: Date;
};

export const cleanUp = async () => {
  const { stdout } = await execPromise(
    `set -x; docker ps -aqf "name=${myContainerName}"`
  );
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
  await cleanUp();
  execSync(`set -x; docker build -t ${myImageName} ${__dirname}`, {
    stdio: "inherit",
  });
  execSync(
    `set -x; docker run -p 5432:5432 --name db -e POSTGRES_PASSWORD=${postgresPassword} -d ${myImageName}:latest`,
    { stdio: "inherit" }
  );
  await new Promise(async (resolve) => {
    while (true) {
      try {
        await knex.select(knex.raw("1"));
        resolve();
        break;
      } catch (err) {
        // console.log("Waiting because:", err.message);
        await wait(1000);
      }
    }
  });
};
const wait = (t: number) => new Promise((y) => setTimeout(y, t));

type ReadSkewOptions = {
  isRepeatableRead?: boolean;
};
/**
 * Reading before and after a transaction has commited violates an application invariant
 */
export async function runReadSkew({ isRepeatableRead }: ReadSkewOptions = {}) {
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
  if (isRepeatableRead) {
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
};
/**
 * Parallel writes on an item that depends on a read violate an application invariant
 */
export async function runWriteSkew({ isSerializable }: WriteSkewOptions = {}) {
  const input = [{ key: "my-key1", value: "0" }];
  await knex<KeyValueTable>("key_value").insert(input);

  const trx = await knex.transaction();
  const trx2 = await knex.transaction();
  if (isSerializable) {
    await trx.raw("set transaction isolation level serializable;");
    await trx2.raw("set transaction isolation level serializable;");
  }
  const value1 = await trx<KeyValueTable>("key_value")
    .select("value")
    .where({ key: "my-key1" })
    .then(([{ value }]) => parseInt(value, 10));
  await trx<KeyValueTable>("key_value")
    .update({ value: value1 + 1 + "" })
    .where({ key: "my-key1" });
  const value2 = await trx2<KeyValueTable>("key_value")
    .select("value")
    .where({ key: "my-key1" })
    .then(([{ value }]) => parseInt(value, 10));
  await trx.commit();
  // This line can't run until commit finishes due to lock held on this
  await trx2<KeyValueTable>("key_value")
    .update({ value: value2 + 1 + "" })
    .where({ key: "my-key1" });
  await trx2.commit();

  const result = await knex<KeyValueTable>("key_value")
    .select("value")
    .where({ key: "my-key1" })
    .then(([{ value }]) => value);

  return { result };
}
