package applications.spout.helper.parser;

//import applications.utils.Configuration;

import applications.util.Configuration;
import applications.util.datatypes.StreamValues;

import java.io.Serializable;
import java.util.List;

public abstract class Parser<T> implements Serializable {
	private static final long serialVersionUID = -1221926672447206098L;
	protected Configuration config;

    public void initialize(Configuration config) {
        this.config = config;
    }

    public abstract T parse(char[] str);

	public abstract List<StreamValues> parse(String value);


	//public abstract List<StreamValues> parse(String[] input);
}