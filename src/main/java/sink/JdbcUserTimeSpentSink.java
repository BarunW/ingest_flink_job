package sink;

import models.UserTimeSpent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;

public class JdbcUserTimeSpentSink extends RichSinkFunction<UserTimeSpent> implements Sink<UserTimeSpent> {
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private transient Connection connection;
    private transient PreparedStatement preparedStatement;

    public JdbcUserTimeSpentSink(String jdbcUrl, String jdbcUser, String jdbcPassword) {
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
    }

    @Override
    public  void open(Configuration parameters) throws Exception {
        Class.forName("org.postgresql.Driver");
        super.open(parameters);
        connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        Statement statement = connection.createStatement();
        String createTableStatement = "CREATE TABLE IF NOT EXISTS user_time_spent(user_id VARCHAR(30), time_spent INTEGER)";
        preparedStatement = connection.prepareStatement("INSERT INTO user_time_spent(user_id, time_spent) VALUES (?,?)");
        statement.execute(createTableStatement);
    }

    @Override
    public void invoke(UserTimeSpent userTimeSpent, Context context) throws Exception {
        preparedStatement.setString(1, userTimeSpent.userId);
        preparedStatement.setLong(2, userTimeSpent.timeSpent);
        preparedStatement.executeUpdate();
    }
    @Override
    public void close() throws Exception {
        super.close();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void write(UserTimeSpent record) throws Exception {
        invoke(record, null);
    }
}
