const extractDni = (text) => text.substring(0, 8);
const extractCbu = (text) => text.substring(8, 30);
const extractAlias = (text) => {
  const aliasText = text.substring(30, 50).trim();

  if (aliasText !== "") {
    return aliasText;
  }
  return "";
};
const extractTypeId = (text) => text.substring(50, 52).trim();
const extractTypeName = (text) => text.substring(52, 70).trim();

const extractCurrency = (texto, inputString) => {
  const startIndex = texto.indexOf(inputString);
  if (startIndex !== -1) {
    const substring = texto.substring(startIndex + inputString.length);
    const match = substring.match(/^\s*(\S{2})/); // Busca los dos primeros caracteres que no son espacios
    return match ? match[1] : "";
  }
  return ""; // Devuelve una cadena vacía si no se encuentra o no hay suficientes caracteres.
};

const extractTextAfterTwoSpaces = (text) => {
  const spacesRegex = /\s{2,}/; // Regular expression to find 2 or more spaces
  const extractedText = text.match(spacesRegex);

  if (extractedText) {
    const start = text.indexOf(extractedText[0]) + extractedText[0].length;
    return text.slice(start);
  }

  return null;
};

// Function to extract the desired number of characters using extractTextAfterTwoSpaces
const extractNumberOfCharacters = (fullText, searchText, characterCount) => {
  const textIndex = fullText.indexOf(searchText);

  if (textIndex !== -1) {
    const textAfter = fullText.slice(textIndex + searchText.length);
    const extractedText = extractTextAfterTwoSpaces(textAfter);

    if (extractedText) {
      return extractedText.substring(0, characterCount) || "";
    }
  }

  return null;
};

const extractCharactersAfterText = (fullText, searchText) => {
  const omittedText = fullText.slice(30); // Skip the first 31 characters
  const textIndex = omittedText.indexOf(searchText);

  if (textIndex !== -1) {
    const textAfter = omittedText.slice(textIndex + searchText.length);
    const extractedCharacters = textAfter.match(/(.+?(?=\s{2,}|\s*$))/);
    if (extractedCharacters) {
      return extractedCharacters[0].trim(); // Return the extracted characters without leading and trailing spaces.
    }
  }

  return null;
};

const extractLetters = (fullText, searchText, characterCount) => {
  const extractedText = extractNumberOfCharacters(
    fullText,
    searchText,
    characterCount
  );

  if (extractedText) {
    // Filtrar solo las letras del texto extraído
    const lettersOnly = extractedText.replace(/[^a-zA-Z]/g, "");

    if (lettersOnly.length > 0) {
      return lettersOnly;
    }
  }

  return "";
};

const findFirstNumericSequence = (text) => {
  // Omitir los primeros 31 caracteres del texto
  const textAfterOmission = text.slice(31);

  const regex = /(\d{11}[A-Za-z])/;
  const match = textAfterOmission.match(regex);

  if (match) {
    const numbers = match[0].match(/\d{11}/);
    return numbers ? numbers[0] : null;
  }

  return null;
};

const extractAndCleanCharacters = (fullText, searchText) => {
  const extractedText = extractCharactersAfterText(fullText, searchText);

  if (extractedText) {
    // Reemplazar comas (',') y barras ('/') con espacios
    const cleanedText = extractedText.replace(/[,/]/g, " ");

    if (cleanedText.length > 0) {
      return cleanedText;
    }
  }

  return null;
};

const extractCreatedDate = (inputString) => {
  // Omitir los primeros 169 caracteres
  const textoSinPrimeros169Caracteres = inputString.slice(169);

  // Encontrar la primera secuencia de 8 caracteres numéricos que no sean todos ceros
  const regex = /([1-9]\d{7})/; // Buscar un número que no comience con ceros
  const match = textoSinPrimeros169Caracteres.match(regex);

  // Verificar si se encontró una secuencia numérica
  const secuenciaNumerica = match ? match[1] : null;

  return secuenciaNumerica;
};

module.exports = {
  extractDni,
  extractCbu,
  extractAlias,
  extractTypeId,
  extractTypeName,
  extractCurrency,
  extractNumberOfCharacters,
  extractCharactersAfterText,
  extractLetters,
  findFirstNumericSequence,
  extractAndCleanCharacters,
  extractCreatedDate,
};
