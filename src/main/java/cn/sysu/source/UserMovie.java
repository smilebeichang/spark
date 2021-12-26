package cn.sysu.source;

/**
 * @Author : song bei chang
 * @create 2021/12/25 12:20
 */
import java.io.Serializable;

public class UserMovie implements Serializable {
    @Override
    public String toString() {
        return "UserMovie{" +
                "userId='" + userId + '\'' +
                ", movieId='" + movieId + '\'' +
                ", ratting=" + ratting +
                '}';
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getMovieId() {
        return movieId;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public Double getRatting() {
        return ratting;
    }

    public void setRatting(Double ratting) {
        this.ratting = ratting;
    }

    public UserMovie() {
    }

    public UserMovie(String userId, String movieId, Double ratting) {
        this.userId = userId;
        this.movieId = movieId;
        this.ratting = ratting;
    }

    private static final long serialVersionUID = 256158274329337559L;

    private String userId;

    private String movieId;

    private Double ratting;

}



