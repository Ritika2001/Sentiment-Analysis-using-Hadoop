import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import org.apache.commons.math3.distribution.NormalDistribution;

public class cdf extends EvalFunc<Double>
{
    @Override
    public Double exec(Tuple input) throws IOException {
        //String replacement = "";
        double rate;
        if (input == null || input.size() == 0)
            return null;    
        float x = (float)input.get(0);//.ToDouble();
        double xmean = (double)input.get(1);
        double xsd = (double)input.get(2);
        double x1 = x;
        NormalDistribution nd = new NormalDistribution(xmean, xsd);
        rate = nd.cumulativeProbability(x1);
        //String finalrate = String.valueOf(rate);
        return rate;
    }
}
