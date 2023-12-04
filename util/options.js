/* eslint-disable no-case-declarations */
const inquirer = require("inquirer");

const selectWorker = async () => {
  const answers = await inquirer.prompt([
    {
      type: "list",
      name: "worker",
      message: "Selecciona un worker",
      choices: [
        "Contabilizar el numero de registros en el archivo bin",
        "Guardar en la dynamo los registros",
        "Guardar los dni en en un json"
      ],
    },
  ]);

  switch (answers.worker) {
    case "Contabilizar el numero de registros en el archivo bin":
      // Opción 1: Usar el perfil default
      return "workers/workerToReadNumberOfRecords.js";
    case "Guardar en la dynamo los registros":
      return "workers/workerToDynamo.js";
    case "Guardar los dni en en un json":
      return "workers/workerKnowingDeletedRecords.js";
    default:
      console.log("Opción no válida.");
      // eslint-disable-next-line no-undef
      process.exit(1);
  }
};

module.exports = { selectWorker };
