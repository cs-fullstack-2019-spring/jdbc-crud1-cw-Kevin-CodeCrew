package com.company;

import java.sql.*;

public class JDBCPSCW {

    private final String url = "jdbc:postgresql://localhost:5432/company";
    private final String user = "postgres";
    private final String password = "postgres";

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public static String rightPadding(String str, int num) {
        return String.format("%1$-" + num + "s", str);
    }

    JDBCPSCW() {
        try {
            /* -------------- DELETE EMPLOYEES --------------*/
            // Clean out any existing Employee records
            dropEmployees();

            /* -------------- INSERT EMPLOYEES --------------*/
            // Step 1: Insert the Employees
            //        Chuck, Jones, MANAGER, 100000
            //        Marty, Krofft, ENGINEER, 80000
            //        Peter, Barker, DEVELOPER, 50000
            //        Sam, Sham, DEVELOPER, 35000
            //        Ellen, Musk, DEVELOPER, 38000
            //        INSERT a new ENGINEER, Thomas Tank, with a salary of 75000
            System.out.println("************* INSERT EMPLOYEES *************");
            insertEmployee("Chuck", "Jones", "MANAGER", 100000);
            insertEmployee("Marty", "Krofft", "ENGINEER", 80000);
            insertEmployee("Peter", "Barker", "DEVELOPER", 50000);
            insertEmployee("Sam", "Sham", "DEVELOPER", 35000);
            insertEmployee("Ellen", "Musk", "DEVELOPER", 38000);
            insertEmployee("Thomas", "Tank", "ENGINEER", 75000);


//        * SELECT all Employee records from the database
            displayEmployees();
//        * SELECT only the DEVELOPERS from the table
            displayByPosition("DEVELOPER");
//        * SELECT only the Employees making more than 50000
            displayEmployeeBySalary(50000);
//        * UPDATE the salary for Ellen Musk and change her salary to 42000
            updateEmployeeSalary("Ellen", "Musk", 42000);
            displayEmployees();
//        * DELETE Sam Sham
            deleteEmployee("Sam", "Sham");
            displayEmployees();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // Display all the Employees
    private void displayEmployees() throws SQLException  {
        String selectSQL = "SELECT * FROM employee";
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = connect();
            pstmt = conn.prepareStatement(selectSQL);

            ResultSet rs = pstmt.executeQuery();
            System.out.println("************* Employee List *************");
            while (rs.next()) {
                System.out.println(String.format("%s\t%s\t%s\t%s\t%s",
                        rs.getString("id"),
                        rightPadding(rs.getString("fname"), 10),
                        rightPadding(rs.getString("lname"), 10),
                        rightPadding(rs.getString("position"), 10),
                        rightPadding(rs.getString("salary"), 10)));
            }

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        } finally {

            if (pstmt != null) {
                pstmt.close();
            }

            if (conn != null) {
                conn.close();
            }

        }
    }

    // Display all the Employees
    private void displayByPosition(String position) throws SQLException {
        String selectSQL = "SELECT * FROM employee where position = ?";
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = connect();
            pstmt = conn.prepareStatement(selectSQL);
            pstmt.setString(1, position);
            ResultSet rs = pstmt.executeQuery();
            System.out.println(String.format("************* %s List *************", position));
            while (rs.next()) {
                System.out.println(String.format("%s\t%s\t%s\t%s\t%s",
                        rs.getString("id"),
                        rightPadding(rs.getString("fname"), 10),
                        rightPadding(rs.getString("lname"), 10),
                        rightPadding(rs.getString("position"), 10),
                        rightPadding(rs.getString("salary"), 10)));
            }

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }finally {

            if (pstmt != null) {
                pstmt.close();
            }

            if (conn != null) {
                conn.close();
            }

        }
    }


    // Display Employees with salary >= value passed in
    private void displayEmployeeBySalary(int salary) throws SQLException {
        String selectSQL = "SELECT * FROM employee where salary >= ?";
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = connect();
            pstmt = conn.prepareStatement(selectSQL);
            pstmt.setInt(1, salary);
            ResultSet rs = pstmt.executeQuery();
            System.out.println(String.format("************* SALARY > = %s *************", salary));
            while (rs.next()) {
                System.out.println(String.format("%s\t%s\t%s\t%s\t%s",
                        rs.getString("id"),
                        rightPadding(rs.getString("fname"), 10),
                        rightPadding(rs.getString("lname"), 10),
                        rightPadding(rs.getString("position"), 10),
                        rightPadding(rs.getString("salary"), 10)));
            }

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }finally {

            if (pstmt != null) {
                pstmt.close();
            }

            if (conn != null) {
                conn.close();
            }

        }
    }


    // Update the Employee with the provided name
    private void updateEmployeeSalary(String fname, String lname, int newSalary) throws SQLException  {
        String deleteSQL = "UPDATE EMPLOYEE SET salary = ? where employee.fname = ? AND employee.lname = ?";
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = connect();
            pstmt = conn.prepareStatement(deleteSQL);
            pstmt.setInt(1,newSalary);
            pstmt.setString(2, fname);
            pstmt.setString(3, lname);
            System.out.println(String.format("******** UPDATED SALARY for EMPLOYEE %s %s ********", fname, lname));
            int rs = pstmt.executeUpdate();
            if (rs > 0) {
                System.out.println("Updated Salary Successfully...");
            } else {
                System.out.println("Employee wasn't found.");
            }
        } catch (
                SQLException ex) {
            System.out.println(ex.getMessage());
        } finally {

            if (pstmt != null) {
                pstmt.close();
            }

            if (conn != null) {
                conn.close();
            }

        }


    }

    // Delete the Employee with the provided name
    private void deleteEmployee(String fname, String lname) throws SQLException {
        String deleteSQL = "DELETE from employee where employee.fname = ? AND employee.lname = ?";
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = connect();
            pstmt = conn.prepareStatement(deleteSQL);
            pstmt.setString(1, fname);
            pstmt.setString(2, lname);
            System.out.println(String.format("************* DROP EMPLOYEE %s %s *************", fname, lname));
            int rs = pstmt.executeUpdate();
            if (rs > 0) {
                System.out.println("Deleted Employee Successfully...");
            } else {
                System.out.println("Employee wasn't found.");
            }
        } catch (
                SQLException ex) {
            System.out.println(ex.getMessage());
        } finally {

            if (pstmt != null) {
                pstmt.close();
            }

            if (conn != null) {
                conn.close();
            }

        }

    }


    // This method takes 4 parameters and createsa new Employee in the database
    private void insertEmployee(String fname, String lname, String position, int salary) throws SQLException {

        String insertSQL = "INSERT INTO employee (fname, lname, position, salary) VALUES (?, ?, ?, ?)";
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = connect();
            pstmt = conn.prepareStatement(insertSQL);
            pstmt.setString(1, fname);
            pstmt.setString(2, lname);
            pstmt.setString(3, position);
            pstmt.setInt(4, salary);
            int rs = pstmt.executeUpdate();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        } finally {

            if (pstmt != null) {
                pstmt.close();
            }

            if (conn != null) {
                conn.close();
            }

        }

    }

    // Convenience method to delete all the Employees
    private void dropEmployees() throws SQLException {

        String deleteSQL = "DELETE from employee";

        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = connect();
            pstmt = conn.prepareStatement(deleteSQL);
            System.out.println("************* DROP EMPLOYEES *************");
            int rs = pstmt.executeUpdate();
            if (rs > 0) {
                System.out.println("Deleted all Employees in the Table Successfully...");
            } else {
                System.out.println("Table is already empty.");
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        } finally {

            if (pstmt != null) {
                pstmt.close();
            }

            if (conn != null) {
                conn.close();
            }

        }
    }
}
