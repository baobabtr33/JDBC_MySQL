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

public class Main {
	private static final String DB_DRIVER = "org.postgresql.Driver";
	private static String DB_CONNECTION_URL;
	private static String SCHEMA_NAME;
	private static String DB_USER;
	private static String DB_PASSWORD;
	
	public static void main(String args[]) throws ClassNotFoundException, SQLException, IOException, FileNotFoundException {
		/* Build connection from connection.txt */
		/* Read the file and store the keywords in appropriate order */
		File file = new File("connection.txt");
		FileReader fileReader = new FileReader(file);
		BufferedReader bufReader = new BufferedReader(fileReader);
		
		ArrayList<String> keywords = new ArrayList<>();
		while (true) {
			String line = bufReader.readLine();
			if (line == null)
				break;
			String parsedKeyword = line.split(":")[1];
			parsedKeyword = parsedKeyword.trim();
			keywords.add(parsedKeyword);
		}
		bufReader.close();
		
		/* Specify information */
		/* and build connection to the database */	
		DB_CONNECTION_URL = "jdbc:postgresql://" + keywords.get(0) + "/" + keywords.get(1);
		SCHEMA_NAME = keywords.get(2);
		DB_USER = keywords.get(3);
		DB_PASSWORD = keywords.get(4);
			
		Class.forName(DB_DRIVER);
		Properties props = new Properties();
		
		props.setProperty("user", DB_USER);
		props.setProperty("password", DB_PASSWORD);
		props.setProperty("currentSchema", SCHEMA_NAME);
		props.setProperty("charterEncoding", "UTF-8");
		props.setProperty("serverTimezone", "Asia/Seoul");
		
		Connection conn = DriverManager.getConnection(DB_CONNECTION_URL, props);
		conn.setAutoCommit(true);
		Statement st = conn.createStatement();
		
		Scanner sc = new Scanner(System.in);
		Manipulator manipulator = new Manipulator();
		
		while (true) {
			System.out.print("Please input the instruction number (1: Import from CSV, 2: Export to CSV, 3: Manipulate Data, "
					+ "4: Exit) : ");
			int command = sc.nextInt();
			sc.nextLine();
			if (command == 1) {
				Parser parser = new Parser();
				
				System.out.println("[Import from CSV]");
				System.out.print("Please specify the filename for table description : ");
				String txtName = sc.nextLine();
				String query = parser.parseText(SCHEMA_NAME, txtName);
				if (manipulator.createTable(st, query))
					System.out.println("Table is newly created as described in the file");
				else
					System.out.println("Table already exists");
				
				System.out.print("Please specify the CSV filename : ");
				String csvName = sc.nextLine();
				
				ArrayList<String[]> data = parser.parseCSV(csvName);
				if (data == null) 
					System.out.println("Data import failure. (The number of columns does not match between "
							+ "the table description and the CSV file.)\n");
				else {
					ArrayList<String> columns = parser.colOrder;
					ArrayList<Integer> errorLog = manipulator.insertMultiple(SCHEMA_NAME, st, parser.tableName, 
																				columns, data);
					
					int success = data.size() - errorLog.size(), failed = errorLog.size();
					System.out.println("Data import completed. (Insertion Success : " + success + ", Insertion Failure : " + failed + ")");
					for (Integer i : errorLog)
						System.out.println("Failed tuple : " + (i + 2) + " line in CSV - " + parser.originalData.get(i));
					System.out.println("");
				}
			}
			else if (command == 2) {
				System.out.println("[Export to CSV]");
				System.out.print("Please specify the table name : ");
				String tableName = sc.nextLine();
				System.out.print("Please specify the CSV filename : ");
				String csvName = sc.nextLine();
				exportCSV(st, tableName, csvName);
			}
			else if (command == 3) {
				System.out.println("[Manipulate Data]");
				while (true) {
					System.out.print("Please input the instruction number (1: Show table, 2: Describe Table, "
							+ "3: Select, 4: Insert, 5: Delete, 6: Update, 7: Drop table, 8: Back to main) : ");
					int instNum = sc.nextInt();
					sc.nextLine();
					if (instNum == 1) {
						System.out.println("==========");
					    System.out.println("Table List");
					    System.out.println("==========");
					    System.out.println(manipulator.showTable(SCHEMA_NAME, st));
					}
					else if (instNum == 2) {
						System.out.print("Please specify the table name : ");
				        String tableName = sc.nextLine();
						System.out.println("==================================================================================");
						System.out.println("Column Name | Data Type | Character Maximum Length(or Numeric Precision and Scale)");
						System.out.println("==================================================================================");
						System.out.println(manipulator.describeTable(SCHEMA_NAME, st, tableName));
					}
					else if (instNum == 3) {
						System.out.print("Please specify the table name : ");
						String tableName = sc.nextLine();
						
						System.out.print("Please specify columns which you want to retrieve (ALL : *) : ");
						String colName = sc.nextLine();
						
						String condition = buildCondition(sc);
						
						System.out.print("Please specify the column name for ordering (Press enter : skip) : ");
						String orderCol = sc.nextLine();
						
						String orderCriteria = "";
						if (!orderCol.equals("")) {
							System.out.print("Please specify the sorting criteria (Press enter : skip) : ");
							orderCriteria = sc.nextLine();
						}
						ArrayList<String[]> result = manipulator.select(SCHEMA_NAME, st, tableName, colName, condition, orderCol, orderCriteria);
						if (result != null) {
							for (int i = 1; i <= result.size(); i++) {
								System.out.println(String.join(", ", result.get(i - 1)));
								if (i % 10 == 0) {
									System.out.print("<Press enter>");
									sc.nextLine();
								}
							}
							if (result.size() > 1)
								System.out.println("<" + result.size() + " rows selected>");
							else
								System.out.println("<" + result.size() + " row selected>");
						}
						else
							System.out.println("<error detected>");
						System.out.println("");
					}
					else if (instNum == 4) {
						System.out.print("Please specify the table name : ");
				        String tableName = sc.nextLine();
						System.out.print("Please specify all columns in order of which you want to insert : ");
				        String colOrder = sc.nextLine();
						System.out.print("Please specify values for each column : ");
				        String colVal = sc.nextLine();
				        if (manipulator.insert(SCHEMA_NAME, st, tableName, colOrder, colVal))
				        	System.out.println("<1 row inserted>\n");
				        else
				        	System.out.println("<0 row inserted due to error>\n");
					}
					else if (instNum == 5) {
						System.out.print("Please specify the table name : ");
				        String tableName = sc.nextLine();
						String condition = buildCondition(sc);
				        int result = manipulator.delete(SCHEMA_NAME, st, tableName, condition);
				        if (result > 1)
				        	System.out.println("<" + result + " rows deleted>\n");
				        else if (result >= 0)
				        	System.out.println("<" + result + " row deleted>\n");
				        else
				        	System.out.println("<error detected>\n");
					}
					else if (instNum == 6) {
						System.out.print("Please specify the table name : ");
					    String tableName = sc.nextLine();
					    String condition = buildCondition(sc);
					    System.out.print("Please specify column names which you want to update : ");
					    String putColumns = sc.nextLine();
					    System.out.print("Please specify the value which you want to put : ");
					    String putValues = sc.nextLine();
					    int result = manipulator.update(SCHEMA_NAME, st, tableName, condition, putColumns, putValues);
					    if (result > 1)
					    	System.out.println("<" + result + " rows updated>\n");
					    else if (result >= 0)
					    	System.out.println("<" + result + " row updated>\n");
					    else
					    	System.out.println("<error detected>\n");
					}
					else if (instNum == 7) {
						System.out.print("Please specify the table name : ");
					    String tableName = sc.nextLine();
					    System.out.print("If you delete this table, it is not guaranteed to recover again. "
					    		+ "Are you sure you want to delete this table (Y: yes, N: no)? ");
					    String answer = sc.nextLine();
					    if(answer.equals("Y")) {
					    	manipulator.dropTable(SCHEMA_NAME, st, tableName);
					        System.out.println("<The table "+tableName+" is deleted>");
					    }
					    else
					       System.out.println("<Deletion canceled>");
					    System.out.println();
					}
					else if (instNum == 8)
						break;
				}
			}
			else
				break;
		}
		sc.close();
		st.close();
	}
	public static String buildCondition(Scanner sc) {
		String query = "";
		System.out.print("Please specify the column which you want to make condition (Press enter : skip) : ");
		String columnName = sc.nextLine();
		if (columnName.equals(""))
			return query;
		query += "\"" + columnName + "\"";
		while (true) {
			System.out.print("Please specify the condition (1: =, 2: >, 3: <, 4: >=, 5: <=, 6: !=, 7: LIKE) : ");
			int num = sc.nextInt();
			sc.nextLine();
			query += " " + mappingCondition(num) + " ";
			
			System.out.print("Please specify the condition value (" + query.replace("\"", "").replace("'", "") + "?) : ");
			String value = sc.nextLine();
			query += "'" + value + "'";
			
			System.out.print("Please specify the condition (1: AND, 2: OR, 3: finish) : ");
			num = sc.nextInt();
			sc.nextLine();
			if (num == 1)
				query += " and ";
			else if (num == 2)
				query += " or ";
			else
				return query;
			System.out.print("Please specify the column which you want to make condition : ");
			columnName = sc.nextLine();
			query += "\"" + columnName + "\"";
		}
	}
	private static String mappingCondition(int num) {
		if (num == 1)
			return "=";
		else if (num == 2)
			return ">";
		else if (num == 3)
			return "<";
		else if (num == 4)
			return ">=";
		else if (num == 5)
			return "<=";
		else if (num == 6)
			return "!=";
		else if (num == 7)
			return "LIKE";
		else
			throw new IllegalArgumentException();
	}
	public static void exportCSV(Statement st, String tableName, String csvName) throws IOException, SQLException{	
		File file = new File(csvName);
		FileWriter fw = new FileWriter(file);
		
		String query = "SELECT * FROM " + "\"" + SCHEMA_NAME + "\"." + "\"" + tableName + "\"";
		ResultSet rs = st.executeQuery(query);
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		String line = rsmd.getColumnName(1);
		for (int i = 2; i <= columnCount; i++)
			line += "," + rsmd.getColumnName(i);
		fw.write(line);
		fw.write(System.lineSeparator());
		while (rs.next()) {
			line = rs.getString(1);
			for (int i = 2; i <= columnCount; i++)
				line += "," + rs.getString(i);
			fw.write(line);
			fw.write(System.lineSeparator());
		}
		System.out.println("Data export completed\n");
		fw.close();
		rs.close();
	}
}