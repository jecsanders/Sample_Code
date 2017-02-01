import _ from 'lodash';
import fs from 'fs';


class DataModifier {
	
	// Function to build standard app data obj
	buildDataObject(input_data) {
	    let columns = [], dataTypes = [], rowArr = [];
		let tables = {};
	    let i = 0;
	    _.forEach(input_data, (col) => {
	        if (i == 0) {
				for (let testVal of Object.values(col)) {
					let valDataType = typeof testVal;
					dataTypes.push(valDataType);
				}
				let c = 0
		        for (let header of Object.keys(col)) {
					let column_type = dataTypes[c];
					if (column_type == 'object') {
						column_type = 'string';
					}
					let column_obj = {'caption': header, 'type': column_type}
			        columns.push(column_obj);
					c++;
		        }
	            rowArr.push(Object.values(col));
	        }
	        else {
		        rowArr.push(Object.values(col));
	        }
        let name = col.TABLE_NAME;
        if(! _.has(tables, name)){
            tables[name] = true;
        }
	    i++;
        });
	    let data_obj = {'columns': columns, 'data_set': rowArr};
	    return data_obj;
	}
}
export default DataModifier;