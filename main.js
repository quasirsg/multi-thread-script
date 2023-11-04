/* eslint-disable no-undef */
const path = require("path");
const Piscina = require("piscina");
const { AbortController } = require("abort-controller");
const abortController = new AbortController();

/** Variables de setup **/
let numberOfPayloads = 0;
const numCores = 2;
const blockSize = 421;
const bytesToSkipStart = 1039;
const bytesToSkipAfter = 250;

const workerOptions = {
  binaryFilePath: "pxldasew.bin", // Reemplaza con la ubicación de tu archivo binario
  blockSize,
  bytesToSkipStart,
  bytesToSkipAfter,
  previousBuffer: Buffer.alloc(0),
  currentOffset: bytesToSkipStart, // Nuevo campo para rastrear la posición actual
};
/** Variables de setup **/

const piscina = new Piscina({
  filename: path.resolve(__dirname, "worker.js"),
  minThreads: numCores,
});

// Manejar los mensajes enviados desde los trabajadores
piscina.on("message", (message) => {
  console.log(numberOfPayloads);
  numberOfPayloads += message;
});

async function runWorker(workerOptions) {
  try {
    const { signal } = abortController;
    const result = await piscina.run(workerOptions, { signal });
    return result;
  } catch (error) {
    console.error("Error en el worker:", error);
  }
}

// Ejecuta x instancias del worker, una para cada núcleo
(async () => {
  console.time("Tiempo de ejecución");
  const workerPromises = Array(numCores)
    .fill()
    .map((_, index) => {
      const threadWorkerOptions = {
        ...workerOptions,
        threadNumber: index,
        offset: index * blockSize,
      };
      return runWorker(threadWorkerOptions);
    });
  const result = await Promise.all(workerPromises);
  console.log(result);
  console.timeEnd("Tiempo de ejecución");
})();
