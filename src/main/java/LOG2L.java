package nodeinfo;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

public class LOG2L extends EvalFunc<Integer>
{
    // FIXME Only returns floored values of log base 2. This means it
    // won't really work for very small or very precise computations.
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
            Long x = (Long) input.get(0);
            return (63 - Long.numberOfLeadingZeros(x));
        }catch(Exception e){
            throw WrappedIOException.
                wrap("In LOG2, caught exception processing input row ", e);
        }
    }
}
