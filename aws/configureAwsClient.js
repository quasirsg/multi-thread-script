/* eslint-disable no-case-declarations */
const inquirer = require("inquirer");

const configureAWSClient = async () => {
  const answers = await inquirer.prompt([
    {
      type: "list",
      name: "authMethod",
      message: "Selecciona una opción de autenticación:",
      choices: [
        "Usar el perfil default",
        "Introducir credenciales manualmente",
        "Introducir qué perfil utilizar",
      ],
    },
  ]);

  switch (answers.authMethod) {
    case "Usar el perfil default":
      // Opción 1: Usar el perfil default
      return { region: "us-east-1" };
    case "Introducir credenciales manualmente":
      // Opción 2: Introducir credenciales manualmente
      const credentialsAnswers = await inquirer.prompt([
        {
          type: "input",
          name: "accessKeyId",
          message: "Introduce tu Access Key ID:",
        },
        {
          type: "input",
          name: "secretAccessKey",
          message: "Introduce tu Secret Access Key:",
        },
        {
          type: "input",
          name: "sessionToken",
          message: "Introduce tu Session Token:",
        },
      ]);

      return {
        region: "us-east-1",
        credentials: {
          accessKeyId: credentialsAnswers.accessKeyId,
          secretAccessKey: credentialsAnswers.secretAccessKey,
          sessionToken: credentialsAnswers.sessionToken,
        },
      };
    case "Introducir qué perfil utilizar":
      // Opción 3: Introducir qué perfil utilizar
      const profileAnswer = await inquirer.prompt([
        {
          type: "input",
          name: "profileName",
          message: "Introduce el nombre del perfil:",
        },
      ]);

      return {
        region: "us-east-1",
        profile: profileAnswer.profileName,
      };
    default:
      console.log("Opción no válida.");
      // eslint-disable-next-line no-undef
      process.exit(1);
  }
};

module.exports = { configureAWSClient };
