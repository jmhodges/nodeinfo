package nodeinfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

// FIXME Only returns floored values of log base 2. This means it
// won't really work for very small or very precise computations.
public class LOG2 extends EvalFunc<Integer>
{
    private static Integer BITS = Integer.SIZE - 1;

    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
            return log(input.get(0));
        }catch(Exception e){
            throw WrappedIOException.
                wrap("In LOG2, caught exception processing input row ", e);
        }
    }

    public Integer log(Object somex) {
        Integer x = (Integer) somex;
        return (BITS - Integer.numberOfLeadingZeros(x));
    }

    static public class DoubleLog2 extends LOG2 {
        private static Integer BITS = Long.SIZE - 1;
        public Integer log(Object somex) {
            Double x = (Double) somex;
            return (BITS - Long.numberOfLeadingZeros(x.longValue()));
        }
    }

    static public class LongLog2 extends LOG2 {
        private static Integer BITS = Long.SIZE - 1;
        public Integer log(Object somex) {
            Long x = (Long) somex;
            return (BITS - Long.numberOfLeadingZeros(x));
        }
    }

    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();

        funcList.add(new FuncSpec(this.getClass().getName(), schema(DataType.INTEGER)));
        funcList.add(new FuncSpec(DoubleLog2.class.getName(), schema(DataType.DOUBLE)));
        funcList.add(new FuncSpec(LongLog2.class.getName(), schema(DataType.LONG)));
        return funcList;
    }

    private Schema schema(byte type) {
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, type));
        return s;
    }
}