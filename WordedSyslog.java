package nodeinfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.FuncSpec;

public class WordedSyslog extends EvalFunc<DataBag> {
    BagFactory mBagFactory = BagFactory.getInstance();
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    int returnTupleSize = 4;
    
    // returns a bag of tuples with word, position, node, and
    // time_bucket in them. position being the position of the word
    // in the line
    public DataBag exec(Tuple input) throws IOException {
        try {
            if (input == null || input.size() == 0) return null;
            
            String line = (String) input.get(0);
            if (line == null) return null;

            Integer interval = null;
            if (input.size() > 1) {
                interval = (Integer) input.get(1);
            }
            if (interval == null) interval = 60;

            DataBag output = mBagFactory.newDefaultBag();
            
            addWordTuples(output, line, interval);
            return output;
        } catch (ExecException ee) {
            // wtf, pig. copy-pasta from TOKENIZE.java
            throw ee;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema bagSchema = new Schema(tupleSchema());
            bagSchema.setTwoLevelAccessRequired(true);
            Schema.FieldSchema bagFS = new Schema.FieldSchema("bag_of_word_tuples",
                                                              bagSchema,
                                                              DataType.BAG);
            return new Schema(bagFS);
        } catch (FrontendException e) {
            // stupidest fucking copy-pasta from TOKENIZE.java
            // throwing RTE because
            //above schema creation is not expected to throw an exception
            // and also because superclass does not throw exception
            throw new RuntimeException("Unable to compute WordedSyslog schema.");
        }   
    }
    
    public Schema.FieldSchema tupleSchema() throws FrontendException {
        Schema s = new Schema();
        s.add(new Schema.FieldSchema("word", DataType.CHARARRAY));
        s.add(new Schema.FieldSchema("position", DataType.INTEGER));
        s.add(new Schema.FieldSchema("node", DataType.CHARARRAY));
        s.add(new Schema.FieldSchema("time_bucket", DataType.CHARARRAY));
        Schema.FieldSchema tupleFS = new Schema.
            FieldSchema("tuple_of_one_word_info",
                        s,
                        DataType.TUPLE);
        return tupleFS;
    }

    private void addWordTuples(DataBag output, String line, Integer interval) throws ExecException {
        StringTokenizer tok = tokenize(line);

        tok.nextToken(); tok.nextToken(); // throw away month and day
        String hour = tok.nextToken();
        Integer minute = Integer.valueOf(tok.nextToken());
        String minStr = String.format("%02d", minute / interval);
        String timeBucket = hour + ":" + minStr;

        tok.nextToken(); // throw away second
        String node = tok.nextToken();

        // throw away the first "word" as it's just the process name.
        // Including it has been found to make the algo useless, so we
        // just don't.
        tok.nextToken();

        int pos = 0;
        while (tok.hasMoreTokens()) {
            Tuple tup = mTupleFactory.newTuple(returnTupleSize);
            tup.set(0, tok.nextToken());
            tup.set(1, pos);
            tup.set(2, node);
            tup.set(3, timeBucket);
            output.add(tup);
            pos++;
        }
    }

    private StringTokenizer tokenize(String line) {
        StringTokenizer tok =  new StringTokenizer(line, " \",()*:", false);
        if (tok.countTokens() < 7) {
            throw new RuntimeException("Malformed syslog line: "+line);
        }
        return tok;
    }

    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));

        Schema t = new Schema();
        t.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        t.add(new Schema.FieldSchema(null, DataType.INTEGER));
        funcList.add(new FuncSpec(this.getClass().getName(), t));
        return funcList;
    }
}