package com.devs4j.kafka.excericise_users_transactions.model;

public class Transaction {
  private Long amount;
  private String type;
  private String timestamp;
  private User user;
  
  public Transaction(String type, Long amount, String timestamp, User user) {
    this.type = type;
    this.amount = amount;
    this.timestamp = timestamp;
    this.user = user;
  }

  public Long getAmount() {
    return amount;
  }

  public void setAmount(Long amount) {
    this.amount = amount;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public User getUser() {
    return user;
  }

  public void setUser(User user) {
    this.user = user;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  @Override
  public String toString() {
    return "Transaction [amount=" + amount + ", timestamp=" + timestamp + ", type=" + type + ", user=" + user.toString() + "]";
  }

  
}
