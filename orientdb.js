// used to avoid sql injection
let validOdbSqlRegex = /(?=^(\w| |:|_|#)+$)(?!.* ([fF][rR][oO][mM]|[Ww][hH][eE][rR][eE]|[lL][iI][mM][iI][tT]|[aA][nN][dD]|[oO][rR]|[rR][aA][nN][gG][eE]|[oO][rR][dD][eE][rR]|[bB][yY])( |$))/gm;
function orientdbsql(request, ...args) {
  let str = "";
  for (let index in request) {
    str += request[index];
    if (args[index]) {
      if (!validOdbSqlRegex.test(args[index])) {
        throw "Arguments contain illegal characters";
      }
      str += args[index];
    }
  }
  return str;
}

module.exports = async function(options = {}) {
  const logger = this.logger;
  if (!options || !options.config) {
    logger.error("No configuration supplied, can't connect to the database");
    throw "No configuration supplied, can't connect to the database";
  }

  const changes = options.changes || [];
  const classes = options.classes || [];

  // Building requests to handle changes like class renaming
  const alterRequests = changes.map(
    ({ name, parents, previous, isEdge, customRequest }) => {
      let r = "";
      if (!previous && parents) {
        r += `CREATE CLASS ${name} IF NOT EXISTS;
            ALTER CLASS ${name} SUPERCLASSES ${parents.join(", ")};`;
      } else {
        if (isEdge) {
          if (name && name != previous.name) {
            r += `ALTER CLASS ${previous.name} NAME ${name} UNSAFE;
                UPDATE V SET out_${name} = out_${previous.name} where out_${previous.name} is not null;
                UPDATE V SET in_${name} = in_${previous.name} where in_${previous.name} is not null;
                UPDATE V REMOVE out_${previous.name} where out_${previous.name} is not null;
                UPDATE V REMOVE in_${previous.name} where in_${previous.name} is not null;`;
          }
        } else {
          if (name && name != previous.name)
            r += `ALTER CLASS ${previous.name} NAME ${name};\n`;
          if (parents)
            r += `ALTER CLASS ${name} SUPERCLASSES ${parents.join(", ")};\n`;
        }
      }
      if (customRequest) r += customRequest;
      return r.replace(/\u00a0/g, " ");
    }
  );

  // Building request to create classes
  const request = `${classes
    .map(
      ({ name, parents, fields }) => `
          CREATE CLASS ${name} IF NOT EXISTS;
          ALTER CLASS ${name} SUPERCLASSES ${parents.join(", ")};
          ${
            fields != undefined
              ? Object.keys(fields)
                  .map(
                    field =>
                      `CREATE PROPERTY ${name}.${field} IF NOT EXISTS ${fields[field]};`
                  )
                  .join("") + "\n"
              : ""
          }`
    )
    .join("")} ${options.returnInitRequestString || "return true;"}`.replace(
    /\u00a0/g,
    " "
  );

  const OrientDBClient = require("orientjs").OrientDBClient;
  const config = options.config;
  let container = {
    client: undefined,
    pool: undefined,
    state: false,
    bootstrapped: false,
    dbExists: false,
    connectIsRunning: false,
    bootstrapperIsRunning: false,
    initialized: false,
    close: async function() {
      if (container.client) {
        try {
          await container.client.close();
          await OrientDBClient.close();
        } catch (e) {}
        container.client = undefined;
      }
      container.connectIsRunning = false;
      container.bootstrapperIsRunning = false;
      container.pool = undefined;
      container.state = false;
      container.bootstrapped = false;
      container.dbExists = false;
    }
  };

  async function connect() {
    if (container.connectIsRunning) return false;
    await container.close();
    container.connectIsRunning = true;
    async function reject() {
      await new Promise(resolve => setTimeout(resolve, 5000));
      await container.close();
      return await connect();
    }
    try {
      container.client = await OrientDBClient.connect(config);
      logger.info("Server link established");
    } catch (e) {
      logger.warn("Server link failed");
      return reject();
    }
    try {
      container.dbExists = await container.client.existsDatabase(config);
      if (container.dbExists) {
        logger.info("Database existed already");
      } else {
        await container.client.createDatabase(config);
        logger.info("Database has been created");
      }
    } catch (e) {
      logger.warn("Database creation failed");
      return reject();
    }
    try {
      container.pool = await container.client.sessions(config);
      container.state = true;
      logger.info("Pool is configured");
    } catch (e) {
      logger.warn("Pool was unable to be configured");
      return reject();
    }
    container.connectIsRunning = false;
    return await bootstrapper();
  }

  async function action(actionProvider, timeout = 1000, rejectError = false) {
    if (!container.state) return false;
    let session;
    try {
      session = await container.pool.acquire();
      let result = await Promise.race([
        actionProvider(session),
        new Promise((_, reject) => {
          setTimeout(_ => reject("timeout"), timeout);
        })
      ]);
      session.close();
      return result;
    } catch (e) {
      logger.debug("Error in concurent access", e);
      if (session) {
        session.close();
      }
      if (rejectError) throw e;
      return false;
    }
  }

  async function updateDB(maxAttempts = 15) {
    if (!alterRequests || alterRequests.length == 0) return true;
    let index = 0;
    let attempts = 0;
    while (index < alterRequests.length) {
      if (alterRequests[index]) {
        await action(s => s.batch(alterRequests[index]).one(), 5000, true)
          .then(r => {
            if (
              options.handleChangeSuccess &&
              typeof options.handleChangeSuccess === "function"
            ) {
              options.handleChangeSuccess(r);
            }
            logger.info("Change has been done");
            index++;
          })
          .catch(error => {
            let changeDone;
            if (
              options.handleChangeError &&
              typeof options.handleChangeError === "function"
            ) {
              changeDone = options.handleChangeError(error);
            } else if (error.code == 5) {
              changeDone = true;
            } else {
              changeDone = false;
            }
            if (changeDone) {
              logger.info("Change has already been done");
              index++;
            } else {
              logger.error("An error happened while updating database", {
                error
              });
              if (attempts < maxAttempts) {
                attempts++;
                index--;
              } else return false;
            }
          });
      }
    }
    return true;
  }

  async function verifyDB(maxAttempts = 15) {
    for (let i = 0; i < maxAttempts; i++) {
      let result = await action(s => s.batch(request).one(), 5000);
      let resultChecked;
      if (
        options.handleVerification &&
        typeof options.handleVerification === "function"
      ) {
        resultChecked = await options.handleVerification(result);
      } else if (result) {
        resultChecked = true;
      }
      if (resultChecked) {
        logger.info(
          "All classes needed exist or have been created, initializing database for other services"
        );
        return true;
      } else {
        logger.warn(
          `Not sure all entities exist in database trying again in 5 seconds. attempts number ${i +
            1}/${maxAttempts}`
        );
        await new Promise(res => setTimeout(res, 5000));
      }
    }
    logger.warn(
      "Error while trying to add all classes and entities in database. bootstrapper canceled..."
    );
    return false;
  }

  async function bootstrapper() {
    if (container.bootstrapperIsRunning) return false;
    container.bootstrapperIsRunning = true;
    if (container.bootstrapped) {
      container.bootstrapped = false;
    }
    const changesDone = await updateDB();
    if (!changesDone) return await container.close();
    const dbVerified = await verifyDB();
    if (!dbVerified) return await container.close();
    container.bootstrapped = true;
    container.bootstrapperIsRunning = false;
    return true;
  }

  setInterval(function() {
    if (container.state && container.bootstrapped) {
      action(s =>
        s.query("SELECT FROM V LIMIT 1".replace(/\u00a0/g, " ")).one()
      ).then(e => {
        if (e === false) {
          logger.warn(`Healthcheck in orientDB database failed`);
          if (!container.connectIsRunning && !container.bootstrapperIsRunning) {
            connect();
          }
        } else {
          logger.info(`Healthcheck in orientDB database was successful`);
        }
      });
    } else {
      if (
        container.initialized &&
        !container.connectIsRunning &&
        !container.bootstrapperIsRunning
      )
        connect();
    }
  }, 20000);

  await connect();
  container.action = action;
  container.bootstrapper = bootstrapper;
  container.orientdbsql = orientdbsql;
  container.initialized = true;
  return container;
};
