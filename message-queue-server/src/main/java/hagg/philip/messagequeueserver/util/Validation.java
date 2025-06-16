package hagg.philip.messagequeueserver.util;


public class Validation {
    public static <T> T nonNull(T object){
        if(object == null)
            throw new NullPointerException("object is null");
        return object;
    }
}
