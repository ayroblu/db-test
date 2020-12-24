import knexBuilder from "knex";
import { promisify } from "util";
import { exec, execSync } from "child_process";
import _ from "lodash";

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
