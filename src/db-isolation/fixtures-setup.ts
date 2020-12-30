import { execSync } from "child_process";
import knexBuilder from "knex";

const pgImageName = "postgres:latest";
const mysqlImageName = "mysql:5.7.32";
const mssqlImageName = "mcr.microsoft.com/mssql/server:2019-latest";
const password = "mysecretpassword";
const mssqlPassword = "yourStrong(!)Password";

const pgKnex = knexBuilder({
  client: "pg",
  connection: {
    host: "localhost",
    user: "postgres",
    password,
    database: "postgres",
  },
});

const mysqlKnex = knexBuilder({
  client: "mysql",
  connection: {
    host: "localhost",
    user: "root",
    password,
    database: "mydb",
  },
});

const mssqlKnex = knexBuilder({
  client: "mssql",
  connection: {
    host: "localhost",
    user: "sa",
    password: mssqlPassword,
    database: "mydb",
  },
});

export type DbType = "pg" | "mysql" | "mssql";

export const getKnex = (dbType: DbType) => {
  switch (dbType) {
    case "pg":
      return pgKnex;
    case "mysql":
      return mysqlKnex;
    case "mssql":
      return mssqlKnex;
    default:
      checkUnreachable(dbType);
      throw new Error("Not Implemented");
  }
};

const checkUnreachable = (_x: never) => {};

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

function getRunCommand(dbType: DbType) {
  switch (dbType) {
    case "pg":
      return `set -x; docker pull -q ${pgImageName}; docker run -p 5432:5432 --name ${dbType} -e POSTGRES_PASSWORD=${password} -d ${pgImageName}`;
    case "mysql":
      return `set -x; docker pull -q ${mysqlImageName}; docker run -p 3306:3306 --name ${dbType} -e MYSQL_DATABASE=mydb -e MYSQL_ROOT_PASSWORD=${password} -d ${mysqlImageName}`;
    case "mssql":
      return `set -x; docker pull -q ${mssqlImageName}; docker run -p 1433:1433 --name ${dbType} -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=${mssqlPassword}' -d ${mssqlImageName}`;
    default:
      checkUnreachable(dbType);
      throw new Error("Not Implemented");
  }
}

export const setupDb = async (dbType: DbType) => {
  await cleanUpDb(dbType);

  const runCommand = getRunCommand(dbType);
  execSync(runCommand, { stdio: "inherit" });

  await waitForDb(dbType);
  await createKeyValueTable(dbType);
};

async function waitForDb(dbType: DbType) {
  const knex = getKnex(dbType);
  if (dbType === "mssql") {
    await waitForMssql();
  }
  return new Promise(async (resolve) => {
    while (true) {
      try {
        await knex.select(knex.raw("1"));
        resolve();
        return;
      } catch (err) {
        // console.warn("Waiting because:", err.message);
        await wait(1000);
      }
    }
  });
}

async function waitForMssql() {
  const knex = knexBuilder({
    client: "mssql",
    connection: {
      host: "localhost",
      user: "sa",
      password: mssqlPassword,
    },
  });
  console.log("Creating mssql mydb");
  async function createDb() {
    while (true) {
      try {
        await knex.raw("CREATE DATABASE mydb");
      } catch (err) {
        await wait(1000);
        continue;
      }
      break;
    }
    await knex.raw("ALTER DATABASE mydb SET ALLOW_SNAPSHOT_ISOLATION ON");
    console.log("Done creating mssql mydb");
  }
  await createDb().then(() => knex.destroy());
}

export const wait = (t: number) => new Promise((y) => setTimeout(y, t));

async function createKeyValueTable(dbType: DbType) {
  const knex = getKnex(dbType);
  return knex.schema.createTable("key_value", (table) => {
    table.string("key");
    table.string("value");
  });
}
