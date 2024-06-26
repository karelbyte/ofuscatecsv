import {
  readFileSync,
  createReadStream,
  createWriteStream,
  existsSync,
  mkdirSync,
} from "fs";
import seedrandom from 'seedrandom';
import csv from "csv-parser";
import * as csvWriter from "fast-csv";

let csvFiles;
let emailcfg;
let email = null;
let seed = null;

if (!existsSync("files")) {
  console.log(`No se encontro la carpeta files`);
  process.exit(1);
}

if (!existsSync("seed.cfg")) {
  console.log(`No se encontro seed.cfg`);
  process.exit(1);
}

try {
  const seeds = readFileSync("seed.cfg", "utf8").split("\n").filter(Boolean);
  if (seeds && seeds.length > 0) {
    seed = seeds[0]
  }
} catch (e) {
  console.log(`No se encontro a seed.cfg`);
  process.exit(1);
}


try {
  csvFiles = readFileSync("files.cfg", "utf8")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
} catch (e) {
  console.log(`No se encontro a files.cfg`);
  process.exit(1);
}

try {
  emailcfg = readFileSync("email.cfg", "utf8").split("\n").filter(Boolean);
  if (emailcfg && emailcfg.length > 0) {
    email = emailcfg[0];
  }
} catch (e) {
  console.log(`No se encontro a email.cfg`);
  process.exit(1);
}

try {
  readFileSync("ofuscate.cfg", "utf8").split("\n").filter(Boolean);
} catch (e) {
  console.log(`No se encontro a ofuscate.cfg`);
  process.exit(1);
}

if (!existsSync("out")) {
  mkdirSync("out");
}

const ofuscate = [];

function scrambleColumn(data, column) {
  const columnx = data.map((row) => row[column - 1]);
  const scrambledColumn = columnx.slice().sort(() => Math.random() - 0.5);
  data.forEach((row, index) => {
    row[column - 1] = scrambledColumn[index];
  });
  return data;
}

function replaceColumn(data, column, term) {
  data.forEach((row) => {
    row[column - 1] = term;
  });
  return data;
}

createReadStream(`ofuscate.cfg`)
  .pipe(csv({ separator: "|", headers: false }))
  .on("data", (row) => {
    ofuscate.push(row);
  })
  .on("end", () => {
    if (ofuscate.length !== csvFiles.length) {
      console.log(
        `La cantidad de archivos a ofuscar no coincide con ofuscate.cfg`
      );
      process.exit(1);
    }
    csvFiles.forEach((file, index) => {
      let masterData = [];
      createReadStream(`files/${file}`)
        .pipe(csv({ separator: "|", headers: false }))
        .on("data", (row) => {
          masterData.push(row);
        })
        .on("end", () => {
          seedrandom(seed, { global: true });
          const columns_to_scramble = ofuscate[index];

          for (const i in columns_to_scramble) {
            masterData = scrambleColumn(masterData, columns_to_scramble[i]);
          }

          if (email !== null && email !== 'null') {
            masterData = replaceColumn(masterData, 92, email);
          } 
      
          const csvFilePath = `out/${file}`;

          const writableStream = createWriteStream(csvFilePath);

          const csvStream = csvWriter.format({ delimiter: "|" });

          csvStream.pipe(writableStream);

          masterData.forEach((row) => csvStream.write(row));

          console.log(`Se creo el archivo ${file}`);

          csvStream.end();
        });
    });
  });
