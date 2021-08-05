#! /usr/bin/env node

const _ = require("lodash");
const fs = require("fs");
const jp = require("jsonpath");
const { program } = require("commander");
const csvParser = require("csv-parser");
const { createArrayCsvWriter } = require("csv-writer");
const { table } = require("table");
const { version } = require("./package.json");

program
  .version(version)
  .option("-i --input <input file>", "the input file (JSON)", "input.json")
  .option("-o --output <output file>", "the output file (CSV)", "output.csv")
  .option("-s --separator <separator>", "separator", ",")
  .option("-q --quiet", "show in stdout", false)
  .option(
    "-m --mapping <mappting file>",
    "the mapping file (CSV)",
    "mapping.csv"
  );

program.parse();

const options = program.opts();

const inputFilePath = options.input;
const outputFilePath = options.output;
const mappingFilePath = options.mapping;
const separator = options.separator;
const quiet = options.quiet;

function readMappings(path, separator) {
  const mappings = {};
  return new Promise((resolve, reject) => {
    fs.createReadStream(path)
      .pipe(csvParser({ separator }))
      .on("data", (data) => {
        _.keys(data).map((key) => {
          let values = mappings[key];
          if (!values) {
            values = [];
            mappings[key] = values;
          }
          values.push(data[key]);
        });
      })
      .on("end", () => {
        resolve(mappings);
      });
  });
}

function readInput(path) {
  return new Promise((resolve, reject) => {
    fs.readFile(path, "utf8", function (err, data) {
      if (err) {
        reject(err);
        return;
      }
      resolve(JSON.parse(data));
    });
  });
}

function parseData(inputData, mappings) {
  return new Promise((resolve, reject) => {
    result = {};
    _.keys(mappings).map((key) => {
      let results = [];
      mappings[key].map((jsonPath) => {
        jsonPath = jsonPath.trim();
        results.push(jp.query(inputData, jsonPath));
      });
      results = _.flatten(results);
      result[key] = results;
    });
    resolve(result);
  });
}

function toTable(data) {
  const someKey = _.head(_.keys(data));
  const someValues = data[someKey];
  let length = someValues.length;
  const header = _.keys(data);
  const rows = [];
  for (i = 0; i < length; i++) {
    const row = [];
    _.keys(data).map((key) => {
      const values = data[key];
      row.push(values[i]);
    });
    rows.push(row);
  }
  return { header, rows };
}

function writeOutput(path, table) {
  const { header, rows } = table;
  const writer = createArrayCsvWriter({ header, path });
  return writer.writeRecords(rows);
}

function showTable(tbl, quiet) {
  if (!quiet) {
    const rows = [];
    rows.push(tbl.header);
    tbl.rows.forEach((row) => rows.push(row));
    console.log(table(rows));
  }
}

Promise.all([
  readInput(inputFilePath),
  readMappings(mappingFilePath, separator),
]).then(([inputData, mappings]) => {
  parseData(inputData, mappings)
    .then((data) => toTable(data))
    .then((table) => {
      writeOutput(outputFilePath, table);
      return table;
    })
    .then((table) => showTable(table, quiet));
});
