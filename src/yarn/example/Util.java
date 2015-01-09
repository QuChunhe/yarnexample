package yarn.example;

public class Util {
    static public boolean isLetter(char ch) {
        if ((ch >= 'a') && (ch <= 'z')) {
            return true;
        }
        if ((ch >= 'A') && (ch <= 'Z')) {
            return true;
        }
        return false;
    }
}
