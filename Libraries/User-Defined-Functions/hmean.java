import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class hmean extends EvalFunc<Double>
{
    @Override
    public Double exec(Tuple input) throws IOException {
        //String replacement = "";
        double rate;
        if (input == null || input.size() == 0)
            return null;    
        double x1 = (double)input.get(0);
        double x2 = (double)input.get(1);
        rate = 2*x1*x2/(x1+x2);
        //String finalrate = String.valueOf(rate);
        return rate;
    }
}
