package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = Util.getConnection();
            statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS `kata2`.`users` (\n" +
                    "  `id` INT NOT NULL AUTO_INCREMENT,\n" +
                    "  `name` VARCHAR(45) NOT NULL,\n" +
                    "  `lastName` VARCHAR(45) NULL,\n" +
                    "  `age` INT(3) NULL,\n" +
                    "  PRIMARY KEY (`id`))\n" +
                    "ENGINE = InnoDB\n" +
                    "DEFAULT CHARACTER SET = utf8;");
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Ошибка при выполнении метода createUsersTable");
        } finally {
            try {
                assert statement != null;
                statement.close();
                connection.close();
            } catch (SQLException e) {
                System.out.println("Ошибка при закрытии ресурсов");
                e.printStackTrace();
            }
        }
    }

    public void dropUsersTable() {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = Util.getConnection();
            preparedStatement = connection.prepareStatement
                    ("DROP TABLE IF EXISTS users");
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Ошибка при выполнении метода dropUsersTable");
            e.printStackTrace();
        } finally {
            try {
                assert preparedStatement != null;
                preparedStatement.close();
                connection.close();
            } catch (SQLException e) {
                System.out.println("Ошибка при закрытии ресурсов");
                e.printStackTrace();
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = Util.getConnection();
            preparedStatement = connection.prepareStatement
                    ("INSERT INTO users (name, lastName, age) VALUES (?, ?, ?);");

            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();
            System.out.println("User с именем " + name + " добавлен в базу данных");

        } catch (SQLException e) {
            System.out.println("Ошибка при выполнении метода saveUser");
            e.printStackTrace();
        } finally {
            try {
                assert preparedStatement != null;
                preparedStatement.close();
                connection.close();
            } catch (SQLException e) {
                System.out.println("Ошибка при закрытии ресурсов");
                e.printStackTrace();
            }
        }
    }

    public void removeUserById(long id) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = Util.getConnection();
            preparedStatement = connection.prepareStatement
                    ("DELETE FROM users WHERE id = ?");
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Ошибка при выполнении метода removeUserById");
            e.printStackTrace();
        } finally {
            try {
                assert preparedStatement != null;
                preparedStatement.close();
                connection.close();
            } catch (SQLException e) {
                System.out.println("Ошибка при закрытии ресурсов");
                e.printStackTrace();
            }
        }
    }


    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = Util.getConnection();
            preparedStatement = connection.prepareStatement
                    ("SELECT * FROM users");
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                String lastName = resultSet.getString("lastName");
                byte age = resultSet.getByte("age");

                User user = new User(name, lastName, age);
                user.setId(id);
                users.add(user);
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при выполнении метода getAllUsers");
            e.printStackTrace();
        } finally {
            try {
                assert preparedStatement != null;
                preparedStatement.close();
                connection.close();
            } catch (SQLException e) {
                System.out.println("Ошибка при закрытии ресурсов");
                e.printStackTrace();
            }
        }
        return users;
    }

    public void cleanUsersTable() {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = Util.getConnection();
            preparedStatement = connection.prepareStatement
                    ("TRUNCATE TABLE users");
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Ошибка при выполнении метода cleanUsersTable");
            e.printStackTrace();
        } finally {
            try {
                assert preparedStatement != null;
                preparedStatement.close();
                connection.close();
            } catch (SQLException e) {
                System.out.println("Ошибка при закрытии ресурсов");
                e.printStackTrace();
            }
        }
    }
}
