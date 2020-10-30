package project2;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.io.BufferedReader;
import java.io.FileNotFoundException;

import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class Manipulator {
	public boolean createTable(Statement st, String createTable) {
		try {
			st.executeUpdate(createTable);
			return true;
		} catch (Exception e){
			return false;
		}
	}
	public String showTable(String schemaName, Statement st) throws SQLException {
	    ResultSet rs = st.executeQuery("SELECT table_name\n" + 
	          "  FROM information_schema.tables\n" + 
	          " WHERE table_schema=" + "'" + schemaName + "'\n" + 
	          "   AND table_type='BASE TABLE';");
	    String result = "";
	    while(rs.next())
	       result += rs.getString(1) + "\n";
	    rs.close();
	    return result;
	}
	
	public String describeTable(String schemaName, Statement st, String tableName) throws SQLException {
        // SQL for column name, data type, max char length(or (numeric precision, scale ))
        ResultSet rs = st.executeQuery("SELECT column_name, data_type, character_maximum_length,numeric_precision, numeric_scale \n" +
                "  FROM INFORMATION_SCHEMA.COLUMNS\n" +
                " WHERE TABLE_SCHEMA = " + "'" + schemaName + "' AND " 
                + "TABLE_NAME = " + "'" + tableName + "'" + "\n" +
                " ORDER BY ORDINAL_POSITION");
        
        String result = "";
        while (rs.next()) {
            result += rs.getString(1) + ", ";
            result += rs.getString(2) + ", ";
            // When the column does not have character like type, return (numeric precision, scale)
            if (rs.getString(3) != null)
            	result += rs.getString(3);
            else if(rs.getString(4) != null)
                result += "(" + rs.getString(4) + "," + rs.getString(5)+")";
            else
            	result = result.substring(0, result.length() - 2);
            result += "\n";
        }
        rs.close();
        return result;
	}
	public ArrayList<String[]> select(String schemaName, Statement st, String tableName, String colName, 
						String condition, String orderCol, String orderCriteria) throws SQLException {
		String query = "SELECT";
		if (colName.equals("*"))
			query += " *";
		else {
			String[] columns = colName.split(",");
			for (String col : columns)
				query += " \"" + col.trim() + "\",";
			query = query.substring(0, query.length() - 1);
		}
		query += " FROM " + "\"" + schemaName + "\"." + "\"" + tableName + "\"";
		if (!condition.equals(""))
			query += " WHERE " + condition;
		if (!orderCol.equals("")) {
			String[] orderColList = orderCol.split(",");
			String[] orderCritList = orderCriteria.split(",");
			query += " ORDER BY";
			for (int i = 0; i < orderColList.length; i++) {
				query += " \"" + orderColList[i].trim() + "\""  + " ";
				if (orderCritList[i].trim().toLowerCase().equals("ascend"))
					query +=  "ASC,";
				else
					query += "DESC,";
				query = query.substring(0, query.length() - 1);
			}
		}
		try {
			ResultSet rs = st.executeQuery(query);
			ResultSetMetaData rsmd = rs.getMetaData();
			int colCount = rsmd.getColumnCount();
			System.out.println("=========================");
			System.out.print(rsmd.getColumnName(1));
			for (int i = 2; i <= colCount; i++)
				System.out.print(" | " + rsmd.getColumnName(i));
			System.out.println("");
			System.out.println("=========================");
			
			ArrayList<String[]> result = new ArrayList<>();
			while (rs.next()) {
				String[] valArr = new String[colCount];
				for (int i = 1; i <= colCount; i++)
					valArr[i - 1] = rs.getString(i);
				result.add(valArr);
			}
			return result;
		} catch (Exception e) {
			return null;
		}
		
	}
	public boolean insert(String schemaName, Statement st, String tableName, String colOrder, String colVal) { 
        // Add comma
        String[] splitOrder = colOrder.split(",");
        String colOrdercomma = "";
        for (String i : splitOrder)
            colOrdercomma += "\"" + i.trim() +"\",";
        colOrdercomma = colOrdercomma.substring(0, colOrdercomma.length() - 1);

        // Add comma - for string need to add '' for when inserting in SQL
        String[] splitVal = colVal.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        String colValcomma = "";
        for (String j : splitVal) {
        	if (j.equals("")) colValcomma += "NULL,";
            else colValcomma += "'" + j.trim() +"'" + ",";
        }
        colValcomma = colValcomma.substring(0, colValcomma.length() - 1);

        String RowInsert = ("INSERT INTO " + "\"" + schemaName + "\"." + "\"" + tableName + "\" " 
        		+ "(" +colOrdercomma + ")" +" VALUES ("+ colValcomma+")");
        try {
            st.executeUpdate(RowInsert);
            return true;
        }
        catch (Exception e)	{
        	return false;
        }
	}
	public ArrayList<Integer> insertMultiple(String schemaName, Statement st, String tableName, ArrayList<String> colName, ArrayList<String[]> data){
		ArrayList<Integer> errorLog = new ArrayList<>();
		String column = String.join(",", colName);
		for (int i = 0; i < data.size(); i++) {
			if (!insert(schemaName, st, tableName, 
									column, String.join(",", data.get(i))))
				errorLog.add(i);
		}
		return errorLog;
	}
	public int delete(String schemaName, Statement st, String tableName, String condition) {
        String deleteSQL = "FROM ";     // SQL delete stmt for this problem
        
        // Delete SQL stmt start
        deleteSQL += "\"" + schemaName + "\"." + "\"" + tableName + "\"";
        if (!condition.equals(""))
        	deleteSQL += " WHERE " + condition;
        deleteSQL = "DELETE " + deleteSQL;
        try {
            return st.executeUpdate(deleteSQL);
        }
        catch (Exception e){
            return -1;
        }
	}
	public int update(String schemaName, Statement st, String tableName, String condition, String putCol, String putVal) {
	    String updateQuery = "UPDATE ";
	    String setQuery = "SET ";
	    updateQuery = updateQuery + "\"" + schemaName +"\"." + "\"" + tableName + "\" ";
	    
	    String[] putColList = putCol.split(",");
	    String[] putValList = putVal.split(",");
	    
	    for (int i = 0; i < putColList.length; i++) {
	       setQuery = setQuery + "\"" + putColList[i].trim() + "\"" + " = '" + putValList[i].trim() + "' ";
	       if(i != putColList.length-1)
	          setQuery = setQuery + ", ";
	    }
	    
	    String resultQuery;
	    if (condition.equals(""))
	       resultQuery = updateQuery + setQuery + ";";
	    else
	       resultQuery = updateQuery + setQuery + "WHERE " + condition + ";";
	    System.out.println(resultQuery);
	    try {
	        return st.executeUpdate(resultQuery);
	    }catch(Exception e) {
	    	return -1;
	    }
	}
	public void dropTable(String schemaName, Statement st, String tableName) throws SQLException {
		st.execute("DROP TABLE" + "\"" + schemaName + "\"." +
				"\""+tableName+"\";");
	}
}
