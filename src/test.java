import java.io.*;
import java.sql.*;
import java.util.Scanner;

public class test {

    public static void loadData(Connection conn, File inputFile, String table) {

        // Delimiter
        String delimiter = "\t";

        // Create scanner
        Scanner scanner;

        try {
            //read line
            BufferedReader br = new BufferedReader(new FileReader(inputFile));
            String line;
            while ((line = br.readLine()) != null) {
                // Prepare statement
                scanner = new Scanner(line).useDelimiter(delimiter);
                PreparedStatement pstmt = null;

                if (table.equals("category")) {
                    //System.out.println("this is category");
                    String statement = "INSERT INTO category VALUES (?,?)";
                    pstmt = conn.prepareStatement(statement);
                }
                else if (table.equals("manufacturer")) {
                    //System.out.println("this is manu");
                    String statement = "INSERT INTO manufacturer VALUES (?,?,?,?,?)";
                    pstmt = conn.prepareStatement(statement);
                }
                else if (table.equals("part")) {
                    //System.out.println("this is part");
                    String statement = "INSERT INTO part VALUES (?,?,?,?,?,?)";
                    pstmt = conn.prepareStatement(statement);
                }
                else if (table.equals("salesperson")) {
//                    System.out.println("this is sale");
                    String statement = "INSERT INTO salesperson VALUES (?,?,?,?)";
                    pstmt = conn.prepareStatement(statement);
                }
                else if (table.equals("transaction")) {
//                    System.out.println("this is tran");
                    String statement = "INSERT INTO transaction VALUES (?,?,?,TO_DATE(?,'DD-MM-YYYY'))";
                    pstmt = conn.prepareStatement(statement);
                }

                int i = 1;
                while (scanner.hasNext()) {
                    pstmt.setString(i, scanner.next());
                    i++;

                }
                //System.out.println(pstmt);
                pstmt.executeUpdate();
                scanner.close();

            }
        } catch (Exception e1) {
            e1.printStackTrace();
            return;
        }




    }

    public static void executeSqlScript(Connection conn, File inputFile) {

        // Delimiter
        String delimiter = ";";

        // Create scanner
        Scanner scanner;
        try {
            scanner = new Scanner(inputFile).useDelimiter(delimiter);
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
            return;
        }

        // Loop through the SQL file statements
        Statement currentStatement = null;
        while(scanner.hasNext()) {

            // Get statement
            String rawStatement = scanner.next();
            //System.out.println(rawStatement);

            if (!scanner.hasNext())
                continue;

            try {
                // Execute statement
                currentStatement = conn.createStatement();
                currentStatement.execute(rawStatement);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                // Release resources
                if (currentStatement != null) {
                    try {
                        currentStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                currentStatement = null;
            }
        }
        scanner.close();
    }

    public static void countRow(Connection conn, String table) {
       try {
           Statement currentStatement = conn.createStatement();
           ResultSet rs = currentStatement.executeQuery("SELECT * FROM "+table);
           int count = 0;
           while (rs.next())
               count++;
           System.out.println(table + ": " + count);
       }
       catch (Exception e) {e.printStackTrace();};

    }

    public static void main(String args[]){

        //connect to database
        Connection connection = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection(
                    "jdbc:oracle:thin:@db12.cse.cuhk.edu.hk:1521:db12", "d129",
                    "siijhuye");
        }

        catch (Exception e) {
            System.out.println(e.getMessage());

        }

        while (1==1)
        {
            System.out.println("Welcome to sales system!\n");

            System.out.println("-----Main menu-----");
            System.out.println("What kind of operation would you like to perform?\n");
            System.out.println("1.Operation for administrator");
            System.out.println("2.Operation for salesperson");
            System.out.println("3.Operation for manager");
            System.out.println("4.Exit this program");
            System.out.print("Enter Your Choice: ");

            Scanner in = new Scanner( System.in );
            int c = in.nextInt();

            // administrator
            if (c == 1){
                int choice = 0;
                while(1==1)
                {
                    System.out.println("\n-----Operation for administrator menu-----");
                    System.out.println("What kind of operation would you like to perform?\n");
                    System.out.println("1.Create all tables");
                    System.out.println("2.Delete all tables");
                    System.out.println("3.Load from datafile");
                    System.out.println("4.Show number of records in each table");
                    System.out.println("5.Return to the main menu");
                    System.out.print("Enter Your Choice: ");
                    choice = in.nextInt();
                    if (choice == 1){

                        File file = null;
                        try {
                            file = new File("schema.sql");
                            executeSqlScript(connection, file);
                            System.out.println("Processing...Done! Database is initialized!");
                        }
                        catch (Exception e){};

                    }
                    else if (choice == 2){

                        File file = null;
                        try {
                            file = new File("delete.sql");
                            executeSqlScript(connection, file);
                            System.out.println("Processing...Done! Database is removed!");
                        }
                        catch (Exception e){};
                    }
                    else if (choice == 3){

                        File file = null;
                        try {
                            file = new File("category.txt");
                            loadData(connection, file, "category");

                            file = new File("manufacturer.txt");
                            loadData(connection, file, "manufacturer");

                            file = new File("part.txt");
                            loadData(connection, file, "part");

                            file = new File("salesperson.txt");
                            loadData(connection, file, "salesperson");

                            file = new File("transaction.txt");
                            loadData(connection, file, "transaction");

                            System.out.println("Processing...Done! Data is input to the database!");
                        }
                        catch (Exception e){};


                    }
                    else if (choice == 4){
                        System.out.println("Number of records in each table:");
                        countRow(connection, "category");
                        countRow(connection, "manufacturer");
                        countRow(connection, "part");
                        countRow(connection, "salesperson");
                        countRow(connection, "transaction");
                    }
                    else if (choice == 5){
                        System.out.println("5.Return...");
                        break;
                    }
                    else {
                        System.out.println("No such choice");
//                    try{ connection.close(); }
//                    catch (Exception err) {};
                        return;
                    }
                }

            }
            // exit program
            else if (c == 4)
                return;

        }

    }
}
