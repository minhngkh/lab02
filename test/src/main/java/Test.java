import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Test {

    public static void main(String[] args) {
        String filePath = "input/bbc/sport/003.txt";
        try {
            String content = new String(Files.readAllBytes(Paths.get(filePath)));
            test(content);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected static void test(String value) throws IOException {

        String[] tokens = value.split("\\s+");

        List<String> processedTokens = new ArrayList<>();
        for (String token : tokens) {
            // Remove punctuations at the end of the token
            token = token.toLowerCase().replaceAll("\\p{Punct}+$", "");

            // Check if the token contains only letters, digits
            // or the £ symbol (but not for things like $ according to bbc.terms ?)
            if (token.matches("^[a-z0-9£]+$")) {
                if (token.equals("0")) {
                    System.out.print("\n" + token + "\n");
                }
                System.out.print(token);
            }
        }
    }
}

