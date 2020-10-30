package project2;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.FileNotFoundException;

public class Parser {
	public String tableName;
	public ArrayList<String> colOrder;
	public ArrayList<String> originalData;
	
	public String parseText(String schemaName, String txtName) throws FileNotFoundException, IOException {
		/*
		 * Function for parsing text file for create table.
		 * Args:
		 * 	String schemaName : Current schema name for database
		 * Return:
		 * 	String createTable : Using this string, the user can create table
		 */
		// Open the text file
		File file = new File(txtName);
		FileReader fileReader = new FileReader(file);
		BufferedReader bufReader = new BufferedReader(fileReader);
		
		// Get table name from the text file
		String line = bufReader.readLine();
		String[] splitted = line.split(":");
		tableName = splitted[1].trim();
		
		// Iterate to get multiple column name and data type
		ArrayList<String> colName = new ArrayList<>();
		ArrayList<String> colDtype = new ArrayList<>();
		while (true) {
			line = bufReader.readLine();
			if (line == null) // If we cannot read the file anymore, break
				break;
			
			splitted = line.split(":");
			splitted[0] = splitted[0].trim(); splitted[1] = splitted[1].trim();
			if (splitted[0].equals("PK") || splitted[0].equals("Not NULL")) // If it is not about column name, break
				break;
			colName.add(splitted[1]);
			
			// Read and fill the data type for current column name
			line = bufReader.readLine();
			String dtype = line.split(":")[1].trim().toLowerCase();
			colDtype.add(dtype);
		}
		colOrder = colName;
		
		String[] PKlist = null;
		String[] NotNulllist = null;
		if (splitted[0].equals("PK")) {
			PKlist = splitted[1].split(",");
			for (int i = 0; i < PKlist.length; i++)
				PKlist[i] = PKlist[i].trim();
			
			line = bufReader.readLine();
			if (line != null) {
				splitted = line.split(":");
				splitted[0] = splitted[0].trim(); splitted[1] = splitted[1].trim();
			}
		}
		if (splitted[0].equals("Not NULL")) {
			NotNulllist = splitted[1].split(",");
			for (int i = 0; i < NotNulllist.length; i++)
				NotNulllist[i] = NotNulllist[i].trim();
		}
		String createTable = "CREATE TABLE " + "\"" + schemaName + "\"." + "\"" + tableName + "\"";
		createTable += " (";
		for (int i = 0; i < colName.size(); i++) {
			createTable += colName.get(i) + " " + colDtype.get(i);
			for (String Notnull: NotNulllist) {
				if (Notnull.equals(colName.get(i)))
					createTable += " " + "not null";
			}
			if (i < colName.size() - 1 || PKlist != null)
				createTable += ", ";
		}
		if (PKlist != null)
			createTable += "primary key (" + String.join(", ", PKlist) + ")";
		createTable += ");";
		
		bufReader.close();
		return createTable;
	}
	public ArrayList<String[]> parseCSV(String csvName) throws FileNotFoundException, IOException {
		/*
		 * Parse CSV file for easier insertion.
		 * Args:
		 * 	None
		 * Return:
		 * 	ArrayList<ArrayList<String>> data
		 */
		ArrayList<String[]> data = new ArrayList<>();
		
		// Open the csv file
		File file = new File(csvName);
		FileReader fileReader = new FileReader(file);
		BufferedReader bufReader = new BufferedReader(fileReader);
		
		String line = bufReader.readLine();
		String[] columns = line.split(",");
		
		int colLen = columns.length;
		int[] columnIndex = new int[colLen];
		if (colLen != colOrder.size()) {
			bufReader.close();
			return null;
		}
		// Determine column order
		for (int i = 0; i < colLen; i++)
			columnIndex[i] = colOrder.indexOf(columns[i].trim());
		
		originalData = new ArrayList<>();
		while((line = bufReader.readLine()) != null) {
			String[] originalDatum = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			String[] sortedDatum = new String[columns.length];
			for (int i = 0; i < colLen; i++)
				sortedDatum[columnIndex[i]] = originalDatum[i];
			data.add(sortedDatum);
			originalData.add(line);
		}
		bufReader.close();
		return data;
	}
}
