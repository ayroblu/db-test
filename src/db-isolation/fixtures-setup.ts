import { execSync } from "child_process";
import knexBuilder from "knex";

const pgImageName = "postgres:latest";
const mysqlImageName = "mysql:5.7.32";
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

export type DbType = "pg" | "mysql";

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
  key: string;
  value: string;
};

export const cleanUpDb = async (dbType: DbType) => {
  const stdout = execSync(`set -x; docker ps -aqf "name=${dbType}"`, {
    encoding: "utf-8",
  });
  const containerId = stdout.trim();
  if (containerId) {
    execSync(`set -x; docker stop "${containerId}" && docker rm "${containerId}"`, {
      stdio: "inherit",
    });
  }
};

export const setupDb = async (dbType: DbType) => {
  const knex = getKnex(dbType);
  await cleanUpDb(dbType);
  const imageName = getImageName(dbType);
  if (dbType === "pg") {
    execSync(
      `set -x; docker run -p 5432:5432 --name ${dbType} -e POSTGRES_PASSWORD=${password} -d ${imageName}`,
      { stdio: "inherit" }
    );
  } else {
    execSync(
      `set -x; docker run -p 3306:3306 --name ${dbType} -e MYSQL_DATABASE=mydb -e MYSQL_ROOT_PASSWORD=${password} -d ${imageName}`,
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
  await createKeyValueTable(dbType);
};

const wait = (t: number) => new Promise((y) => setTimeout(y, t));

async function createKeyValueTable(dbType: DbType) {
  const knex = getKnex(dbType);
  return knex.schema.createTable("key_value", (table) => {
    table.string("key");
    table.string("value");
  });
}
