import java.util.*;
import java.io.*;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class SeparateStringWords extends EvalFunc<String>{
    public String exec(Tuple input) throws IOException {
        String replacement = "";
        String correct;
        if (input == null || input.size() == 0)
            return null;    
        try{
        separatestrings alag = new separatestrings();
        String str = input.get(0).toString();
        correct = alag.a(str);
        return correct;
}
    catch (Exception e){
        throw new IOException("Caught exception processing input", e);
        }
    
    }

public static class separatestrings{
    public String a(String str){
        BufferedReader bf1 = null;
    BufferedReader bf2 = null;
    
    try{
        Set < String > dict = new HashSet < String > ();
        String line;
        bf1 = new BufferedReader(new FileReader("/home/ritika/NLP/Libraries/stopwords-en.txt"));
        bf1.readLine();
        while ((line = bf1.readLine()) != null) {
            //if (line.length()>1)
            {dict.add(line);}
                }
        bf2 = new BufferedReader(new FileReader("/home/ritika/NLP/Libraries/dict.txt"));
        bf2.readLine();
        while ((line = bf2.readLine()) != null) {
            //if (line.length()>2)
            {dict.add(line);}
                }
        dict.add("i");
        
            String separated = segmentString(str, dict);
        return separated;
            }
    
    catch (Exception e){e.printStackTrace();
return null;}

       
    }
    
    private String segmentString(String str, Set < String > dict) {

    
        if (dict.contains(str)) return str;
        int len = str.length();

        for (int i = 1; i < len; i++) {
            String prefix = str.substring(0, i);
            if (dict.contains(prefix)) {
                String suffix = str.substring(i, len); //StringIndexOutOfBoundException 
                String subSuffix = segmentString(suffix, dict);
                if (subSuffix != null) {
                    return prefix + " " + subSuffix;

                }
            }
        }
        return null;
    }
}
}

