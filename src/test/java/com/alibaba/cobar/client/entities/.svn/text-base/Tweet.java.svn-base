package com.alibaba.cobar.client.entities;

public class Tweet {
    private Long id;
    private String tweet;
    
    public Tweet(){}
    
    public Tweet(String t){
        tweet = t;
    }
    
    public Long getId() {
        return id;
    }
    public void setId(Long id) {
        this.id = id;
    }
    public String getTweet() {
        return tweet;
    }
    public void setTweet(String tweet) {
        this.tweet = tweet;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((tweet == null) ? 0 : tweet.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Tweet other = (Tweet) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (tweet == null) {
            if (other.tweet != null)
                return false;
        } else if (!tweet.equals(other.tweet))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Tweet [id=" + id + ", tweet=" + tweet + "]";
    }
    
}
