package models;

public class UserTimeSpent {
    public String userId;
    public long timeSpent;

    public UserTimeSpent() {}

    public UserTimeSpent(String userId, long timeSpent) {
        this.userId = userId;
        this.timeSpent = timeSpent;
    }
}
