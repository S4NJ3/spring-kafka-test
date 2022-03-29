package com.devs4j.kafka.excericise_users_transactions.model;

public class User {
  
  private Integer id;
  
  
  public User(Integer id) {
    this.id = id;
  }

  public Integer getId() {
    return id;
  }
  
  public void setId(Integer id) {
    this.id = id;
  }

  @Override
  public String toString() {
    return "User [id=" + id + "]";
  }
  
  
}
