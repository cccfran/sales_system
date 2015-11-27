import java.io.*;
import java.sql.*;
import java.util.*;
import java.lang.*;
import java.text.*;

public class SalesSystem {
    static Scanner in = new Scanner( System.in );

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
    
    public static void search(Connection conn) {
        System.out.println("Choose the Search criterion:");
        System.out.println("1. Part Name");
        System.out.println("2. Manufacturer Name");
        System.out.print("Choose the search criterion: ");  
        switch (in.nextInt()){
            case 1: searchByPart(conn); break;
            case 2: searchByManufacturer(conn); break;
        }
    }
    
    public static void searchByPart(Connection conn) {
        System.out.print("Type in the Search Keyword: ");        
        String keyword = in.next();
        System.out.println("Choose ordering:");
        System.out.println("1. By price, ascdending order");
        System.out.println("2. By price, descending order");
        System.out.print("Choose the search criterion: ");
        String order;
        switch (in.nextInt()) {
            case 1: order = "ASC"; break;
            case 2: order = "DESC"; break;
            default: order = "ASC"; break;
        }
        
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            
            ResultSet rs = stmt.executeQuery("SELECT * FROM (part "
                    + "INNER JOIN manufacturer ON (part.mID=manufacturer.mID) "
                    + "LEFT OUTER JOIN category on (part.cID=category.cID)) "
                    + "WHERE pName LIKE '%" + keyword+ "%' "
                    + "ORDER BY pPrice " + order);
            
            System.out.println("| ID | Name | Mnufacturer "
                + "| Category | Quantity | Warranty | Price |");
            
            while (rs.next())
                System.out.println("| " + rs.getInt("pID") + " | " + rs.getString("pName")
                    + " | " + rs.getString("mName") + " | " + rs.getString("cName")
                    + " | " + rs.getInt("pAvailableQuantity") + " | "
                    + rs.getInt("mWarrantlyPeriod") + " | " + rs.getInt("pPrice"));

            System.out.println("End of Query");
        } //catch (NullPointerException nlp) {    
            //System.out.println("No part named " + keyword);
        //}
        catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
        } catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally{
            System.out.println();
            try{
                if(stmt != null)
                   stmt.close();
             } catch(SQLException se){
             }
        }
    }
    
    public static void searchByManufacturer(Connection conn) {
        System.out.print("Type in the Search Keyword: ");        
        String keyword = in.next();
        System.out.println("Choose ordering:");
        System.out.println("1. By price, ascdending order");
        System.out.println("2. By price, descending order");
        System.out.print("Choose the search criterion: ");
        String order;
        switch (in.nextInt()) {
            case 1: order = "ASC"; break;
            case 2: order = "DESC"; break;
            default: order = "ASC"; break;
        }
        
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            
            ResultSet rs = stmt.executeQuery("SELECT * FROM (part "
                    + "INNER JOIN manufacturer ON (part.mID=manufacturer.mID) "
                    + "LEFT OUTER JOIN category on (part.cID=category.cID)) "
                    + "WHERE mName LIKE '%" + keyword+ "%' "
                    + "ORDER BY pPrice " + order);
            
            System.out.println("| ID | Name | Mnufacturer "
                + "| Category | Quantity | Warranty | Price |");
            
            while (rs.next())
                System.out.println("| " + rs.getInt("pID") + " | " + rs.getString("pName")
                    + " | " + rs.getString("mName") + " | " + rs.getString("cName")
                    + " | " + rs.getInt("pAvailableQuantity") + " | "
                    + rs.getInt("mWarrantlyPeriod") + " | " + rs.getInt("pPrice"));
        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally{
            System.out.println();
            try{
                if(stmt != null)
                   stmt.close();
             }catch(SQLException se){
             }
        }
    }
    
    private static java.sql.Date getCurrentDate() {
        java.util.Date today = new java.util.Date();
        return new java.sql.Date(today.getTime());
    }
    
    public static void sell(Connection conn) {
        System.out.print("Enter The Part ID: ");
        int pid = in.nextInt();
        System.out.print("Enter The Salesperson ID: ");
        int sid = in.nextInt();
        
        Statement stmt = null;
        PreparedStatement updateStmt = null;
        try {
            stmt = conn.createStatement();
            
            // Query salesperson of sid
            ResultSet rs = stmt.executeQuery("SELECT * FROM salesperson WHERE sID=" + sid);
            if (!rs.next()) {
                System.out.println("No salesperson of sid " + sid);
                return; 
            }

            rs = stmt.executeQuery("SELECT * FROM part WHERE pID=" + pid);
            if (!rs.next()) {
                System.out.println("No part of pid " + pid);
                return; 
            }
            // Query part of pid
            rs = stmt.executeQuery("SELECT pAvailableQuantity, pName "
                                            + "FROM part WHERE pID=" + pid);
            while (rs.next()) {
                // check availability
                int availableQuantity = rs.getInt("pAvailableQuantity");
                if (availableQuantity == 0) {
                    System.out.println(rs.getString("pName") + " is sold out");
                    return;
                } else {
                    // find next tID
                    int currentTID;
                    rs = stmt.executeQuery("SELECT MAX(tID) as max FROM transaction");
                    if (rs.next())
                        currentTID = rs.getInt("max") + 1;
                    else
                        currentTID = 1;

                    // Insert new transaction
                    updateStmt = conn.prepareStatement("INSERT INTO transaction "
                                                + "VALUES (?,?,?,?)");
                    updateStmt.setInt(1, currentTID);
                    updateStmt.setInt(2, pid);
                    updateStmt.setInt(3, sid);
                    updateStmt.setDate(4, getCurrentDate());
                    updateStmt.executeUpdate();
                    conn.commit();

                    // Update available quantity in part
                    stmt.executeUpdate("UPDATE part "
                                    + "SET pAvailableQuantity=" + --availableQuantity
                                    + " WHERE pID=" + pid);
                }
            }
            
            // print out an informative message
            rs = stmt.executeQuery("SELECT pName, pID, pAvailableQuantity "
                                + "FROM part WHERE pID=" + pid);
            while (rs.next()) {
                System.out.println("Product: " + rs.getString("pName") +"(id: "
                                + rs.getInt("pID") + ") Remaining Quality: " 
                                + rs.getInt("pAvailableQuantity"));
            }
        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            System.out.println();
            try{
                if(stmt != null)
                   stmt.close();
                if (updateStmt != null)
                    updateStmt.close();
             }catch(SQLException se){}       
        }  
    }
    
    public static void returnMain() {
        System.out.println();
    }
    
    
    
    public static void salesRecord(Connection conn){
        System.out.print("Enter The Salesperson ID: ");
        int sid = in.nextInt();
        Scanner scan = new Scanner(System.in);
        System.out.print("Type in the starting date [dd/mm/yyyy]: ");
        String stDate = scan.nextLine();
        System.out.print("Type in the ending date [dd/mm/yyyy]: ");
        String endDate = scan.nextLine();
        
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
        
        Statement stmt = null;
        
        
        try{
            stmt = conn.createStatement();

            //ResultSet rs = stmt.executeQuery("SELECT * FROM transaction NATURAL JOIN salesperson "
            //                                 + "WHERE sID= " + sid + " AND tDate >=" + stDate + " AND tDate <= " +  endDate + " ORDER BY tDate DESC" );
            
            ResultSet rs = stmt.executeQuery("SELECT *"
                                             + "FROM Manufacturer M, Part P, Transaction T "
                                             + "WHERE T.sID = " + sid  + " AND P.pid = T.pid AND M.mid = P.mid AND "
                                             + "T.tDate >= to_date('"+ stDate +"','DD/MM/YYYY') AND "
                                             + "T.tDate <= to_date('"+ endDate + "','DD/MM/YYYY') " +
                                             "ORDER BY T.tDate DESC");
            
            System.out.println("Transaction Record:");
            System.out.println("| ID | Part ID | Part Name | Mnufacturer | Price | Date |");
            
            while (rs.next())
                System.out.println("| " + rs.getInt("tID") + " | " + rs.getInt("pID") + " | " + rs.getString("pName")
                                   + " | " + rs.getString("mName") + " | " + rs.getInt("pPrice") + " | " +  formatter.format(rs.getDate("tDate")) + " | " );
            
            System.out.println("End of Query");
        }
        catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            System.out.println();
            try{
                if(stmt != null)
                    stmt.close();
            }catch(SQLException se){}
        }
        
    }
    
    public static void totalSales(Connection conn){
        Statement stmt = null;
        try{
            stmt = conn.createStatement();

            //ResultSet rs = stmt.executeQuery("SELECT T.mID, T.mName "+
            //                                 "FROM Transaction T, Part P, Manufacturer M " +
            //                                 "WHERE P.pid = T.pid AND M.mid = P.mid " +
            //                                 "GROUP BY T.mID ");
            ResultSet rs = stmt.executeQuery("SELECT M.mid, M.mName, SUM (Temp.pSum) AS sum FROM Manufacturer M RIGHT JOIN ("+
                                             "SELECT P.mid, P.pID, (P.pPrice * Temp2.count) AS pSum " +
                                             "FROM Part P INNER JOIN (" +
                                             "SELECT pID, COUNT(*) AS count FROM Transaction GROUP BY pID) Temp2 ON P.pID = Temp2.pID) Temp " +
                                             "ON M.mid = Temp.mid GROUP BY M.mid, M.mName " +
                                             "ORDER BY sum DESC");

           // ResultSet rs = stmt.executeQuery("SELECT Temp.t_mid, SUM (Temp.t_price) AS sum FROM (SELECT M.mID AS t_mid, P.pPrice AS t_price, M.mName AS t_mname FROM Transaction T, Part P, Manufacturer M WHERE P.pid = T.pid AND M.mid = P.mid) Temp GROUP BY Temp.t_mid");
           // ResultSet rs = stmt.executeQuery("SELECT Manufacturer.mPhoneNumber, COUNT (Manufacturer.mAddress)"
             //                                + "FROM Manufacturer "
               //                              + "GROUP BY Manufacturer.mID");

            System.out.println("| Manufacturer ID | Manufacturer Name | Total Sales Value |");
            
            while (rs.next())
                System.out.println("| " + rs.getInt("mID") + " | " + rs.getString("mName") + " | " + rs.getInt("sum")
                                   + " | " );

            System.out.println("End of Query");
        }
        
        catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            System.out.println();
            try{
                if(stmt != null)
                    stmt.close();
            }catch(SQLException se){}
        }
    }
    
    public static void popular(Connection conn){
        Statement stmt = null;
        System.out.print("Type in the number of parts: ");
        int num = in.nextInt();
        
        try{
            //ResultSet rs = stmt.executeQuery("SELECT * COUNT(*) AS count "
            //      + "FROM transaction NATURAL JOIN part "
            //      + "GROUP BY pID "
            //      + "ORDER BY count DESC");
            stmt = conn.createStatement();

            
            
            ResultSet rs = stmt.executeQuery("SELECT Part.pID, Part.pName, count FROM Part, (" +
                                             "SELECT Transaction.pID AS t_pID, COUNT (Transaction.pID) AS count " +
                                             "FROM Transaction GROUP BY Transaction.pID) WHERE Part.pID = t_pID ORDER BY count DESC");
            
            System.out.println("| Part ID | Part Name | No. of Transaction |");
            
            int counter = 1;
            
            while (rs.next() && counter <= num){
                System.out.println("| " + rs.getInt("pID") + " | " + rs.getString("pName") + " | " + rs.getInt("count") + " | "  );
                counter = counter + 1;}
                
            System.out.println("End of Query");
        }
        
        catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            System.out.println();
            try{
                if(stmt != null)
                    stmt.close();
            }catch(SQLException se){}
        }
        
    }


    public static void main(String args[]){
        //connect to database
        Connection connection = null;
        try {
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
                            break;
			}
                        catch (Exception e){};
                    }
                    else if (choice == 2){

                        File file = null;
                        try {
                            file = new File("delete.sql");
                            executeSqlScript(connection, file);
                            System.out.println("Processing...Done! Database is removed!");
                            break;
                        }
                        catch (Exception e){};
                    }
                    else if (choice == 3){

                        System.out.print("Type in the Source Data Folder Path: ");
			Scanner scan = new Scanner(System.in);
			String path = scan.next();
			//System.out.println(path);	
			File file = null;
                        try {
                            file = new File(path+"/category.txt");
                            loadData(connection, file, "category");

                            file = new File(path+"/manufacturer.txt");
                            loadData(connection, file, "manufacturer");

                            file = new File(path+"/part.txt");
                            loadData(connection, file, "part");

                            file = new File(path+"/salesperson.txt");
                            loadData(connection, file, "salesperson");

                            file = new File(path+"/transaction.txt");
                            loadData(connection, file, "transaction");

                            System.out.println("Processing...Done! Data is input to the database!");
                            break;
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
                        break;
                    }
                    else if (choice == 5){
                        //System.out.println("5.Return...");
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
            
            // salesperson
            else if (c == 2) {
                System.out.println("\n----Operations for salesperson menu-----");
                System.out.println("What kinds of operation would you like to perform?");
                System.out.println("1. Search for parts");
                System.out.println("2. Sell a part");
                System.out.println("3. Return to the main menu");
                System.out.print("Enter Your Choice: ");

                switch(in.nextInt()) {
                    case 1: search(connection); break;
                    case 2: sell(connection); break;
                    case 3: returnMain(); break;
                    default: System.out.println("Input error"); break;
                }
            }
            
            // manager
            else if (c == 3) {
                System.out.println("\n----Operations for manager menu-----");
                System.out.println("What kinds of operation would you like to perform?");
                System.out.println("1. Show the sales record of a salesperson within a period");
                System.out.println("2. Show the total sales value of each manufacturer");
                System.out.println("3. Show the N most popular part");
                System.out.println("4. Return to the main menu");
                System.out.print("Enter Your Choice: ");
                
                switch(in.nextInt()) {
                    case 1: salesRecord(connection); break;
                    case 2: totalSales(connection); break;
                    case 3: popular(connection); break;
                    case 4: returnMain(); break;
                    default: System.out.println("Input error"); break;
                }
            }

            // exit program
            else if (c == 4)
                return;

        }

    }
        catch (Exception err)
        {
            err.printStackTrace();
        }
    }
}
