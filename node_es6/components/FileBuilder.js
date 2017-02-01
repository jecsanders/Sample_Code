import fs from 'fs';
import nodeExcel from 'excel-export';


class FileBuilder {
	
	// Function to ceate Excel File
	createExcelFile(dataObj, fileName) {
		let conf = {
			cols: dataObj.columns,
			//stylesXmlFile: "PathToStyleSheet", Optional: appends styleSheet xml to Excel
			name: "test",
			rows: dataObj.data_set
		};
		let result = nodeExcel.execute(conf);
		fs.writeFileSync(fileName, result, 'binary');
		return "Excel File Created!"
	}
	
	// Function to create CSV File
	createCsvFile(dataObj, fileName) {
		let headerLength = 0, numRows=0;
	    let csvStr = '';
		for (let header of dataObj.columns) {
			if (headerLength < dataObj.columns.length - 1) {
			    csvStr += header.caption + ',';
			}
			else {
				csvStr += header.caption + '\n';
			}
			headerLength++;		
		}
		for (let row of dataObj.data_set) {
			let csvLength = 0;
			for (let record of row) {
			    if (csvLength < row.length - 1) {
				    csvStr += record + ',';
			    }
				else {
					csvStr += record + '\n';
				}
				csvLength++;
			}
		}
        fs.writeFileSync(fileName, csvStr, 'binary');
		return "CSV File Created";	
	}
}
export default FileBuilder;
