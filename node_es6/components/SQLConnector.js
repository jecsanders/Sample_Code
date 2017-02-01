import sql from 'mssql';


class SQLConnector {
	
  // Constructor
  constructor(config){
    this.config = config;
  }
  
  // Connect to Database
  connect(callback){
    sql.connect(this.config).then(callback);
  }
  
  // Execute SQL Query against connected database
  query(query){
    return (onSuccess,onFail) => {
      this.connect(() => {
        new sql.Request().query(query)
        .then((recordset) => {
          onSuccess(recordset);
          sql.close()
        })
        .catch((error) => {
          console.error('I fucked up',error);
          //lol like that ever happens
          if(onFail){
            onFail()
          }
          sql.close();
        });
      });
    }
  }
  
  // Hard coded function to get column by tale metadata
  getColumnNames(){
    return this.query("SELECT * FROM INFORMATION_SCHEMA.columns;")
  }
  
  // Hard coded function to get table data
  getTableNames(){
    return this.query("SELECT name FROM sys.tables;")
  }
  
  // Hard coded function to return list of tables with records
  getTablesWithData(){
	return this.query("CREATE TABLE #temp (table_name sysname, row_count INT, reserved_size VARCHAR(50), data_size VARCHAR(50), index_size VARCHAR(50), unused_size VARCHAR(50)) SET NOCOUNT ON INSERT #temp EXEC sp_msforeachtable 'sp_spaceused ''?''' SELECT a.table_name, a.row_count, COUNT(*) AS col_count, a.data_size FROM #temp a INNER JOIN information_schema.columns b ON a.table_name collate database_default = b.table_name collate database_default WHERE a.row_count > 0 GROUP BY a.table_name, a.row_count, a.data_size ORDER BY CAST(REPLACE(a.data_size, ' KB', '') AS integer) DESC DROP TABLE #temp;") 
  }
}
export default SQLConnector;
