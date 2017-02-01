import SQLConnector from 'components/SQLConnector';
import DataModifier from 'components/DataModifier';
import FileBuilder from 'components/FileBuilder';


import _ from 'lodash';

let config = {
  server: 'server',
  port: 1433,
  user: 'username',
  password: 'password',
  database: 'db_name'
}
console.log("\n");
console.log("***************************");
console.log("START SQL Query TESTS: ");
console.log("***************************");
console.log("\n");

let conn = new SQLConnector(config);

new Promise(conn.getColumnNames())
  .then( (recordset) => {
	let modifier = new DataModifier();
	let builder = new FileBuilder();
	let dataObj = modifier.buildDataObject(recordset);
	console.log(dataObj)
	console.log(builder.createExcelFile(dataObj, 'results/test.xlsx'));
    console.log(builder.createCsvFile(dataObj, 'results/test.csv'));
	})
  .catch((e) => {console.log('oops ' + e)}); 
