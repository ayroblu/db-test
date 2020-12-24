import knexBuilder from "knex";
import { promisify } from "util";
import { exec, execSync } from "child_process";
import _ from "lodash";

const execPromise = promisify(exec);
const myImageName = "db-tests/pg-repeatable-read";
const myContainerName = "db";
const postgresPassword = "mysecretpassword";

const knex = knexBuilder({
  client: "pg",
  connection: {
    host: "127.0.0.1",
    user: "postgres",
    password: postgresPassword,
    database: "postgres",
  },
});
interface KeyValueTable {
  id: string;
  key: string;
  value: string;
  created_at: Date;
}
async function run() {
  await setup();

  console.log("Running");
  const input = {
    key: "my-key",
    value: "my-value",
  };
  await knex<KeyValueTable>("key_value").insert(input);
  const selectResult = await knex<KeyValueTable>("key_value").select().where("key", input.key);
  console.log(
    "result in db",
    _.isEqual(
      selectResult.map(({ key, value }) => ({
        key,
        value,
      })),
      [input]
    )
  );

  await cleanUp();
  process.exit();
}
async function cleanUp() {
  const { stdout } = await execPromise(`set -x; docker ps -aqf "name=${myContainerName}"`);
  const containerId = stdout.trim();
  if (containerId) {
    execSync(`set -x; docker stop "${containerId}" && docker rm "${containerId}"`, {
      stdio: "inherit",
    });
  }
}
async function setup() {
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
        console.log("Waiting because:", err.message);
        await wait(1000);
      }
    }
  });
}
run().catch(console.error);
const wait = (t: number) => new Promise((y) => setTimeout(y, t));
// docker build -t db-tests/pg-repeatable-read .
// docker run -p 5432:5432 --name db -e POSTGRES_PASSWORD=mysecretpassword -d --restart unless-stopped db-tests/pg-repeatable-read:latest
// ID=$(docker ps -qf "name=$1")
// [ ! -z "$ID" ] && docker stop "$ID" && docker rm "$ID"
// knex.insert({key, value})
// knex.select().where key = key
