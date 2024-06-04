package sink;

import models.UserTimeSpent;

public class SinkFactory {
    public static Sink<UserTimeSpent> createJdbcSink(String jdbcUrl, String jdbcUser, String jdbcPassword) {
        return new JdbcUserTimeSpentSink(jdbcUrl, jdbcUser, jdbcPassword);
    }
}
