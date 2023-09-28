const DynamoDBClient = require("@aws-sdk/client-dynamodb").DynamoDBClient;
const DynamoDBDocumentClient =
  require("@aws-sdk/lib-dynamodb").DynamoDBDocumentClient;
const BatchWriteCommand = require("@aws-sdk/lib-dynamodb").BatchWriteCommand;
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const XLSX = require("xlsx");
// Configura la región de AWS (si es necesario)
const s3Client = new S3Client({
  region: "us-east-1", // Reemplaza con tu región de AWS
});

const params = {
  Bucket: "mtabancaria",
  Key: "mctabancaria-muestra.xlsx", // Reemplaza con la ubicación del archivo .xlsx
};

const client = new DynamoDBClient({ region: "us-east-1" });
const ddbDocClient = DynamoDBDocumentClient.from(client);
const tableName = "accounts-cbus-develop-table";
(async () => {
  try {
    let totalRegistrosEscritos = 0;
    // Obtiene el objeto desde S3
    const { Body } = await s3Client.send(new GetObjectCommand(params));

    const bufferData = Buffer.from(await streamToBuffer(Body));

    // Parsea el ArrayBuffer como archivo XLSX
    const workbook = XLSX.read(bufferData);

    // Accede a una hoja específica (por ejemplo, la primera hoja)
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];

    // Convierte la hoja en un arreglo de objetos JSON
    const data = XLSX.utils.sheet_to_json(sheet, { header: 1 });

    // Define el tamaño de página (por ejemplo, 100 líneas por página, en este caso 25 por que el BatchWriteCommand permite solo 25 registros por operación)
    const pageSize = 25;

    // Inicializa el índice de página
    let pageIndex = 0;
    const batchSize = 100; // Tamaño del lote
    console.log(
      `Este script necesita que tengas configuradas tus credenciales de aws en tu pc `
    );
    console.log(`Comenzando la escritura por lotes en la tabla ${tableName}`);
    while (pageIndex * pageSize < data.length) {
      // Calcula el rango de líneas para la página actual
      const startIndex = pageIndex * pageSize;
      const endIndex = Math.min((pageIndex + 1) * pageSize, data.length);

      // Divide las líneas en lotes de tamaño batchSize
      for (
        let batchStart = startIndex;
        batchStart < endIndex;
        batchStart += batchSize
      ) {
        const batchEnd = Math.min(batchStart + batchSize, endIndex);

        // Crea un arreglo para almacenar los objetos de datos del lote actual
        const jsonResult = [];

        // Itera sobre las líneas del lote actual (comenzando desde la tercera fila, fila 2)
        for (let i = batchStart + 2; i < batchEnd; i++) {
          const row = data[i];
          // Convierte a cadena (string) los valores de row[0] y row[7]
          const customer_id = row[0].toString();
          const bank_decription = row[7].toString();
          // Crea un objeto con nombres de propiedad específicos
          const record = {
            customer_id,
            customer_id_cbu: `${customer_id}:${row[1]}`,
            account_bank: {
              type: {
                id: row[3],
                name: row[4],
              },
              bank: {
                id: row[6],
                name: bank_decription,
              },
              alias: row[2],
              cbu: row[1],
              owner: {
                type: row[8],
              },
              currency: row[5],
            },
          };

          // Agrega el objeto al arreglo JSONResult
          jsonResult.push(record);
        }
        // Crea un lote de escritura para DynamoDB
        const batchWriteParams = {
          RequestItems: {
            // Nombre de la tabla de DynamoDB
            [tableName]: jsonResult.map((item) => ({
              PutRequest: {
                Item: item,
              },
            })),
          },
        };

        await ddbDocClient.send(new BatchWriteCommand(batchWriteParams));
        const registrosEscritosEnLote = jsonResult.length;
        totalRegistrosEscritos += registrosEscritosEnLote; // Actualiza el contador

        console.log(
          `${registrosEscritosEnLote} registros escritos en este lote`
        );
      }
      // Incrementa el índice de página
      pageIndex++;
    }
    console.log(`${totalRegistrosEscritos} total de registros escritos`);
  } catch (error) {
    console.error("Error al obtener o procesar el archivo desde S3:", error);
  }
})();

// Función para convertir un flujo de datos en un Buffer
async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}
