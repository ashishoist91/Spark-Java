import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MySQLoHiveDataIngestion {

	
	private static final Logger logger = Logger.getLogger(MySQLoHiveDataIngestion.class.getName());
	private static Connection conection = null;
	private static Map<String,String> columnDetails= new HashMap<String,String>();
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		System.setProperty("hadoop.home.dir", "E:\\Hadoop");
		
		logger.info("Calling main Method");
		logger.info("Creating Java Spark Context");
		JavaSparkContext javaSparkContext = new JavaSparkContext("local", "MySQLoHiveDataIngestion");
		logger.info("Creating SQLContext");
		//SparkContext sqlContext = new org.apache.spark.sql.SQLContext(javaSparkContext);
		HiveContext hiveContext = new HiveContext(javaSparkContext);
		Properties props = new Properties();
		props.setProperty("user", "admin");
		props.setProperty("password", "admin");
		//Dataset<Row> result = hiveContext.read().jdbc("jdbc:mysql://localhost:3306/hadoop", "transaction", props);;
		
		DataFrame result = hiveContext.read().jdbc("jdbc:mysql://localhost:3306/hadoop", "transaction", props);
		
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", "jdbc:mysql://localhost:3306/hadoop");
		options.put("dbtable", "hadoop.transaction");
		options.put("user", "root");
		options.put("password", "root");
		
		DataFrame frame = hiveContext.read().format("jdbc").options(options).load();
		String columns [] = frame.columns();
		for(String column : columns) {
			logger.info("Column Name :  "+ column);
			//logger.info("Data :  "+ frame.collectAsList().forEach(r -> System.out.println(r)));
		}
		
		DataFrame createNewDataFrame = hiveContext.createDataFrame(new ArrayList<Row>(), getSchema());
		logger.info("Creating Empty DataFrame");
		createNewDataFrame.printSchema();
		
		//result.show();
		//result.printSchema();
		//Dataset<Row> newResult = result.wi
		//conection = MySQLoHiveDataIngestion.getConnection();
		//String createTable = "CREATE TABLE IF NOT EXISTS " + " transaction ("
		//;
		conection = getConnection();
		String createTable  = MySQLoHiveDataIngestion.getHiveTableQuery();
		DataFrame newTable = hiveContext.sql(createTable);
		
		
		List<Row> arrayList= new ArrayList<Row>();
		arrayList = result.collectAsList();
		
		
		//newTable.withColumn(result, col)
		newTable.printSchema();
	}
	
	public static Connection getConnection() throws ClassNotFoundException, SQLException {
		
		Class.forName("com.mysql.jdbc.Driver");  
		Connection conection=DriverManager.getConnection(  
		"jdbc:mysql://localhost:3306/hadoop","root","root");  
		return conection;
		
	}
	
	
	public static String getHiveTableQuery() throws SQLException {
		
		Statement st = conection.createStatement();
		ResultSet rs = st.executeQuery("SELECT * FROM transaction");
		String createTable = "CREATE TABLE IF NOT EXISTS " + " transaction (";
		ResultSetMetaData rsMetaData = rs.getMetaData();
		for(int i=1;i<=rsMetaData.getColumnCount();i++) {
			columnDetails.put(rsMetaData.getColumnName(i), rsMetaData.getColumnTypeName(i));
			logger.info("Column Name " + rsMetaData.getColumnName(i));
			logger.info("Column DataType " + rsMetaData.getColumnTypeName(i));
			//logger.info("Column DataType " + rsMetaData.);
			//org.apache.spark.sql.types.DataType
			
			createTable += rsMetaData.getColumnName(i) + " " +
							getHiveDataType(rsMetaData.getColumnTypeName(i)) + ",";	
			
			
		}
		
		createTable = createTable.trim().substring(0, createTable.length()-1) + ") STORED AS PARQUET" ;
		
		logger.info("Create Table Statement : " + createTable);
		conection.close();
		return createTable;
		
	}
	
	
	public static String getHiveDataType(String dataType) {
		
		if (dataType == "STRING" || dataType == "CHAR" || dataType == "VARCHAR2" || dataType == "VARCHAR") {
	        return "STRING";
	    } else if (dataType == "INT") {
	        return "INT";
	    } else if (dataType == "LONG") {
	        return "BIGINT";
	    } else if (dataType == "FLOAT") {
	        return "FLOAT";
	    } else if (dataType == "DOUBLE") {
	        return "DOUBLE";
	    } else if (dataType == "BOOLEAN") {
	        return "TINYINT";
	    } else if (dataType == "BYTE") {
	        return "SMALLINT";
	    } else if (dataType == "DECIMAL") {
	    	return "DECIMAL";
	    }else if (dataType == "DATE") {
	    	return "DATE";
	    }
		
		
		return null;
	}
	
	public static DataType getDataType(String dataType) {
		
		if (dataType == "STRING" || dataType == "CHAR" || dataType == "VARCHAR2" || dataType == "VARCHAR") {
	        return  DataTypes.StringType;
	    } else if (dataType == "INT") {
	        return DataTypes.IntegerType;
	    } else if (dataType == "LONG") {
	        return DataTypes.LongType;
	    } else if (dataType == "FLOAT") {
	        return DataTypes.FloatType;
	    } else if (dataType == "DOUBLE") {
	        return DataTypes.DoubleType;
	    } else if (dataType == "BOOLEAN") {
	        return DataTypes.BooleanType;
	    } else if (dataType == "BYTE") {
	        return DataTypes.BooleanType;
	    } else if (dataType == "DECIMAL") {
	    	return DataTypes.DoubleType;
	    }else if (dataType == "DATE") {
	    	return DataTypes.DateType;
	    }
		
		
		return null;
	}
	
	
	public static StructType getSchema() {

	    String schemaString = "column1 column2 column3 column4 column5";

	    List<StructField> fields = new ArrayList<StructField>();

	    StructField field = null;

	    for (Map.Entry<String,String> entry : columnDetails.entrySet()) {
	    	 System.out.println("Key = " + entry.getKey() +
                     ", Value = " + entry.getValue());
	    	 
	    	 field = DataTypes.createStructField(entry.getKey(), getDataType(entry.getValue()), true);
	    	 
	    }
           
	    
	    
//	    for (String fieldName : schemaString.split(" ")) {
//	        StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
//	        fields.add(field);
//	    }

	    StructType schema = DataTypes.createStructType(fields);

	    return schema;
	}
	
	
	
	
	

}
