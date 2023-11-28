/* eslint-disable no-undef */
const path = require("path");
const Piscina = require("piscina");
const { configureAWSClient } = require("./aws/configureAwsClient");

/** Variables de setup **/
let numberOfPayloads = 0;
const numCores = 8;
const blockSize = 421;
const bytesToSkipStart = 1039;
const workerToDynamoPath = path.resolve(__dirname, "workers/workerToDynamo.js");
const workerKnowingDeletedRecords = path.resolve(__dirname, "workers/workerKnowingDeletedRecords.js");

const workerOptions = {
  binaryFilePath: "files/pxldasew.bin",
  blockSize,
  bytesToSkipStart,
};
/** Variables de setup **/

const piscina = new Piscina({
  filename: workerKnowingDeletedRecords,
  maxThreads: numCores,
});

// Manejar los mensajes enviados desde los trabajadores
piscina.on("message", (recordsWrited) => {
  numberOfPayloads += recordsWrited;
  console.log(numberOfPayloads);
});

// Ejecuta x instancias del worker, una para cada núcleo
(async () => {
  console.time("Tiempo de ejecución");
  // Crear un arreglo de promesas para ejecutar los trabajadores
  const option = await configureAWSClient();
  const workerPromises = Array(numCores)
    .fill()
    .map(async (_, i) => {
      try {
        // Ejecutar el trabajador en el grupo de Piscina
        const result = await piscina.run({
          ...workerOptions,
          threadNumber: i,
          option,
        });
        return result;
      } catch (error) {
        console.error(`Error en el trabajador ${i}:`, error);
      }
    });

  const workerResults = await Promise.all(workerPromises);
  console.log(workerResults);
  console.timeEnd("Tiempo de ejecución");
})();
