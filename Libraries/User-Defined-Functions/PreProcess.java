import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;
import java.util.*;
public class PreProcess extends EvalFunc<String>
{
    public String exec(Tuple input) throws IOException {
        String replacement = "";
        String correct;
        if (input == null || input.size() == 0)
            return null;    
        String misspelled = input.get(0).toString();
        correct = misspelled.replaceAll("(.)\\1{1,}", "$1");
        return correct;
    }
}
