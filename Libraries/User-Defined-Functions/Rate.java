import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class Rate extends EvalFunc<Float>
{
    @Override
    public Float exec(Tuple input) throws IOException {
        String replacement = "";
        float rate;
        if (input == null || input.size() == 0)
            return null;    
        long freq1 = (long)input.get(0);
        long freq2 = (long)input.get(1);
        rate = (float)freq1/freq2;
        return rate;
    }
}

