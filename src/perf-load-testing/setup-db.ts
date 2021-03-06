import knexSetup from "knex";

export async function setupDb() {
  const dbName = `TestDb_${makeId(10)}`;
  await knex.raw(`create database ${dbName}`);
  console.log(`Set up database: ${dbName}`);

  return {
    dbName,
    knex: knexSetup({
      ...knexConfig,
      connection: { ...knexConfig.connection, database: dbName },
    }),
  };
}
export async function teardownDb(dbName: string) {
  await knex.raw(`drop database ${dbName}`);
  console.log(`torn down database: ${dbName}`);
}

const knexConfig = {
  client: "pg",
  connection: {
    host: "127.0.0.1",
    user: "docker",
    password: "todo",
  },
};

const knex = knexSetup(knexConfig);

function makeId(length: number) {
  var characters = "abcdefghijklmnopqrstuvwxyz";
  var charactersLength = characters.length;
  return Array(length)
    .fill(null)
    .map(() => characters.charAt(Math.floor(Math.random() * charactersLength)))
    .join("");
}
