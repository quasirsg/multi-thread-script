/* eslint-disable no-undef */
const path = require("path");
const Piscina = require("piscina");
const { configureAWSClient } = require("./aws/configureAwsClient");
const { selectWorker } = require("./util/options");

/** Variables de setup **/
let numberOfPayloads = 0;
const numCores = 8;
const blockSize = 421;
const bytesToSkipStart = 1039;

// Manejar los mensajes enviados desde los trabajadores

// Ejecuta x instancias del worker, una para cada núcleo
(async () => {
  const filePath = await selectWorker();
  const option = await configureAWSClient();
  console.log(filePath);
  const workerOptions = {
    binaryFilePath: "files/pxldasew2.bin",
    blockSize,
    bytesToSkipStart,
  };
  /** Variables de setup **/

  const piscina = new Piscina({
    filename: path.resolve(__dirname, filePath),
    maxThreads: numCores,
  });

  piscina.on("message", (recordsWrited) => {
    numberOfPayloads += recordsWrited;
    console.log(numberOfPayloads);
  });

  console.time("Tiempo de ejecución");
  // Crear un arreglo de promesas para ejecutar los trabajadores
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
  const numberOfRecords = workerResults.reduce((acumulador, numero) => acumulador + numero, 0);
  console.log(`Numero de registros: ${numberOfRecords}`);
  console.timeEnd("Tiempo de ejecución");
})();
