package applications.ml.datatypes.instance;


import applications.ml.datatypes.Attribute;
import applications.ml.datatypes.Range;
import applications.ml.datatypes.Utils;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * A list of training data instances
 */
public class Instances {

    /**
     * The keyword used to denote the start of an arff header
     */
    public final static String ARFF_RELATION = "@relation";

    /**
     * The keyword used to denote the start of the arff data section
     */
    public final static String ARFF_DATA = "@data";

    private static final long serialVersionUID = 8110510475535581577L;
    /**
     * The instance information.
     */
    protected InstanceInformation instanceInformation;
    /**
     * The instances.
     */
    protected List<Instances> instances;

    protected static enum AVRO_ENCODING_FORMAT {
        JSON, BINARY
    }

    protected int classAttribute;

    /**
     * A Hash that stores the indices of features.
     */
    protected HashMap<String, Integer> hsAttributesIndices;

    transient protected Loader loader;

    /**
     * Instantiates a new instances.
     *
     * @param chunk the chunk
     */
    public Instances(Instances chunk) {
        this(chunk, chunk.numInstances());
        chunk.copyInstances(0, this, chunk.numInstances());
    }

    /**
     * Instantiates a new instances.
     */
    public Instances() {
    }

    /**
     * Instantiates a new instances.
     *
     * @param reader         the reader
     * @param size           the size
     * @param classAttribute the class attribute
     */
    public Instances(Reader reader, int size, int classAttribute) {
        this.loader = new ArffLoader(reader, 0, classAttribute);
        this.instanceInformation = loader.getStructure();
        this.instances = new ArrayList<Instances>();
    }

    /**
     * Instantiates a new instances.
     *
     * @param reader the reader
     * @param range
     */
    public Instances(Reader reader, Range range) {
        this.loader = new ArffLoader(reader, 0, classAttribute);//new MultiTargetArffLoader(reader, range);
        this.instanceInformation = loader.getStructure();
        this.instances = new ArrayList<Instances>();
    }

    public Instances(InputStream inputStream, int classAttribute, String encodingFormat) {
        this.classAttribute = classAttribute;

        if (encodingFormat.equalsIgnoreCase(AVRO_ENCODING_FORMAT.BINARY.toString()))
            loader = new AvroBinaryLoader(inputStream, classAttribute);
        else
            loader = new AvroJsonLoader(inputStream, classAttribute);

        this.instanceInformation = loader.getStructure();
        this.instances = new ArrayList<>();
    }

    /**
     * Instantiates a new instances.
     *
     * @param chunk    the chunk
     * @param capacity the capacity
     */
    public Instances(Instances chunk, int capacity) {
        this.instanceInformation = chunk.instanceInformation();
        if (capacity < 0) {
            capacity = 0;
        }
        this.instances = new ArrayList<Instances>(capacity);
    }

    /**
     * Instantiates a new instances.
     *
     * @param st       the st
     * @param v        the v
     * @param capacity the capacity
     */
    public Instances(String st, List<Attribute> v, int capacity) {
        this.instanceInformation = new InstanceInformation(st, v);
        this.instances = new ArrayList<Instances>(capacity);
    }

    /**
     * Instantiates a new instances.
     *
     * @param chunk  the chunk
     * @param first  the first instance
     * @param toCopy the j
     */
    public Instances(Instances chunk, int first, int toCopy) {

        this(chunk, toCopy);

        if ((first < 0) || ((first + toCopy) > chunk.numInstances())) {
            throw new IllegalArgumentException("Parameters first and/or toCopy out "
                    + "of range");
        }
        chunk.copyInstances(first, this, toCopy);
    }

    /**
     * Instantiates a new instances.
     *
     * @param st       the st
     * @param capacity the capacity
     */
    public Instances(StringReader st, int capacity) {
        this.instances = new ArrayList<Instances>(capacity);
    }

    //Information Instances

    /**
     * Sets the relation name.
     *
     * @param string the new relation name
     */
    public void setRelationName(String string) {
        this.instanceInformation.setRelationName(string);
    }

    /**
     * Gets the relation name.
     *
     * @return the relation name
     */
    public String getRelationName() {
        return this.instanceInformation.getRelationName();
    }

    /**
     * Class index.
     *
     * @return the int
     */
    public int classIndex() {
        return this.instanceInformation.classIndex();
    }

    /**
     * Sets the class index.
     *
     * @param classIndex the new class index
     */
    public void setClassIndex(int classIndex) {
        this.instanceInformation.setClassIndex(classIndex);
    }

    /**
     * Class attribute.
     *
     * @return the attribute
     */
    public Attribute classAttribute() {
        return this.instanceInformation.classAttribute();
    }

    /**
     * Num attributes.
     *
     * @return the int
     */
    public int numAttributes() {
        return this.instanceInformation.numAttributes();
    }

    /**
     * Attribute.
     *
     * @param w the w
     * @return the attribute
     */
    public Attribute attribute(int w) {
        return this.instanceInformation.attribute(w);
    }

    /**
     * Num classes.
     *
     * @return the int
     */
    public int numClasses() {
        return this.instanceInformation.numClasses();
    }

    /**
     * Delete attribute at.
     *
     * @param integer the integer
     */
    public void deleteAttributeAt(Integer integer) {
        this.instanceInformation.deleteAttributeAt(integer);
    }

    /**
     * Insert attribute at.
     *
     * @param attribute the attribute
     * @param i         the i
     */
    public void insertAttributeAt(Attribute attribute, int i) {
        if (this.instanceInformation == null) {
            this.instanceInformation = new InstanceInformation();
        }
        this.instanceInformation.insertAttributeAt(attribute, i);
    }

    //List of Instances

    /**
     * Instances.
     *
     * @param num the num
     * @return the instance
     */
    public Instances instance(int num) {
        return this.instances.get(num);
    }

    /**
     * Num instances.
     *
     * @return the int
     */
    public int numInstances() {
        return this.instances.size();
    }

    /**
     * Adds the.
     *
     * @param inst the inst
     */
    public void add(Instances inst) {
        this.instances.add(inst);
    }

    /**
     * Randomize.
     *
     * @param random the random
     */
    public void randomize(Random random) {
        for (int j = numInstances() - 1; j > 0; j--) {
            swap(j, random.nextInt(j + 1));
        }
    }

    /**
     * Stratify.
     *
     * @param numFolds the num folds
     */
    public void stratify(int numFolds) {

        if (classAttribute().isNominal()) {

            // sort by class
            int index = 1;
            while (index < numInstances()) {
                Instances instance1 = instance(index - 1);
                for (int j = index; j < numInstances(); j++) {
                    Instances instance2 = instance(j);
                    if ((instance1.classValue() == instance2.classValue())
                            || (instance1.classIsMissing()
                            && instance2.classIsMissing())) {
                        swap(index, j);
                        index++;
                    }
                }
                index++;
            }
            stratStep(numFolds);
        }
    }

    protected void stratStep(int numFolds) {
        ArrayList<Instances> newVec = new ArrayList<Instances>(this.instances.size());
        int start = 0, j;

        // create stratified batch
        while (newVec.size() < numInstances()) {
            j = start;
            while (j < numInstances()) {
                newVec.add(instance(j));
                j = j + numFolds;
            }
            start++;
        }
        this.instances = newVec;
    }

    /**
     * Train cv.
     *
     * @param numFolds the num folds
     * @param numFold
     * @param random   the random
     * @return the instances
     */
    public Instances trainCV(int numFolds, int numFold, Random random) {
        Instances train = trainCV(numFolds, numFold);
        train.randomize(random);
        return train;
    }

    public Instances trainCV(int numFolds, int numFold) {
        int numInstForFold, first, offset;
        Instances train;

        numInstForFold = numInstances() / numFolds;
        if (numFold < numInstances() % numFolds) {
            numInstForFold++;
            offset = numFold;
        } else {
            offset = numInstances() % numFolds;
        }
        train = new Instances(this, numInstances() - numInstForFold);
        first = numFold * (numInstances() / numFolds) + offset;
        copyInstances(0, train, first);
        copyInstances(first + numInstForFold, train,
                numInstances() - first - numInstForFold);
        return train;
    }

    protected void copyInstances(int from, Instances dest, int num) {
        for (int i = 0; i < num; i++) {
            dest.add(instance(from + i));
        }
    }

    /**
     * Test cv.
     *
     * @param numFolds the num folds
     * @param numFold  the num fold
     * @return the instances
     */
    public Instances testCV(int numFolds, int numFold) {

        int numInstForFold, first, offset;
        Instances test;

        numInstForFold = numInstances() / numFolds;
        if (numFold < numInstances() % numFolds) {
            numInstForFold++;
            offset = numFold;
        } else {
            offset = numInstances() % numFolds;
        }
        test = new Instances(this, numInstForFold);
        first = numFold * (numInstances() / numFolds) + offset;
        copyInstances(first, test, numInstForFold);
        return test;
    }

  /*  public Instances dataset() {
     throw new UnsupportedOperationException("Not yet implemented");
     }*/

    /**
     * Mean or mode.
     *
     * @param j the j
     * @return the double
     */
    public double meanOrMode(int j) {
        throw new UnsupportedOperationException("Not yet implemented"); //CobWeb
    }

    /**
     * Read instance.
     *
     * @param fileReader the file reader
     * @return true, if successful
     */
    public boolean readInstance(Reader fileReader) {

        //ArffReader arff = new ArffReader(reader, this, m_Lines, 1);
        if (loader == null) {
            loader = new ArffLoader(fileReader, 0, this.classAttribute);
        }
        Instances inst = loader.readInstance();
        if (inst != null) {
            inst.setDataset(this);
            add(inst);
            return true;
        } else {
            return false;
        }
    }

    public boolean readInstance() {

        Instances inst = loader.readInstance();
        if (inst != null) {
            inst.setDataset(this);
            add(inst);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Delete.
     */
    public void delete() {
        this.instances = new ArrayList<Instances>();
    }

    /**
     * Delete.
     */
    public void delete(int index) {
        this.instances.remove(index);
    }

    /**
     * Swap.
     *
     * @param i the i
     * @param j the j
     */
    public void swap(int i, int j) {
        Instances in = instances.get(i);
        instances.set(i, instances.get(j));
        instances.set(j, in);
    }

    /**
     * Instances information.
     *
     * @return the instance information
     */
    private InstanceInformation instanceInformation() {
        return this.instanceInformation;
    }

    public Attribute attribute(String name) {

        for (int i = 0; i < numAttributes(); i++) {
            if (attribute(i).name().equals(name)) {
                return attribute(i);
            }
        }
        return null;
    }

    public int size() {
        return this.numInstances();
    }

    public void set(int i, Instances inst) {
        this.instances.set(i, inst);
    }

    public Instances get(int k) {
        return this.instance(k);
    }

    public void setRangeOutputIndices(Range range) {
        this.instanceInformation.setRangeOutputIndices(range);

    }

    public void setAttributes(List<Attribute> v) {
        if (this.instanceInformation == null) {
            this.instanceInformation = new InstanceInformation();
        }
        this.instanceInformation.setAttributes(v);
    }

    public void setAttributes(List<Attribute> v, List<Integer> indexValues) {
        if (this.instanceInformation == null) {
            this.instanceInformation = new InstanceInformation();
        }
        this.instanceInformation.setAttributes(v, indexValues);
    }

    /**
     * Returns the dataset as a string in ARFF format. Strings are quoted if
     * they contain whitespace characters, or if they are a question mark.
     *
     * @return the dataset in ARFF format as a string
     */
    public String toString() {

        StringBuffer text = new StringBuffer();

        text.append(ARFF_RELATION).append(" ").
                append(Utils.quote(this.instanceInformation.getRelationName())).append("\n\n");
        for (int i = 0; i < numAttributes(); i++) {
            text.append(attribute(i).toString()).append("\n");
        }
        text.append("\n").append(ARFF_DATA).append("\n");

        text.append(stringWithoutHeader());
        return text.toString();
    }

    /**
     * Returns the dataset as a string in ARFF format. Strings are quoted if
     * they contain whitespace characters, or if they are a question mark.
     *
     * @return the dataset in ARFF format as a string
     */
    public String toStringArff() {

        StringBuffer text = new StringBuffer();

        text.append(ARFF_RELATION).append(" ").
                append(Utils.quote(this.instanceInformation.getRelationName())).append("\n\n");
        for (int i = 0; i < numAttributes(); i++) {
            text.append(attribute(i).toString()).append("\n");
        }
        text.append("\n").append(ARFF_DATA).append("\n");

        text.append(stringWithoutHeader());
        return text.toString();
    }

    /**
     * Returns the instances in the dataset as a string in ARFF format. Strings
     * are quoted if they contain whitespace characters, or if they are a
     * question mark.
     *
     * @return the dataset in ARFF format as a string
     */
    protected String stringWithoutHeader() {

        StringBuffer text = new StringBuffer();

        for (int i = 0; i < numInstances(); i++) {
            text.append(instance(i));
            if (i < numInstances() - 1) {
                text.append('\n');
            }
        }
        return text.toString();

    }

    /**
     * Returns the index of an Attribute.
     *
     * @param att, the attribute.
     */
    protected int indexOf(Attribute att) {
        if (this.hsAttributesIndices == null || !this.hsAttributesIndices.containsKey(att.name)) {
            computeAttributesIndices();
        }

        return this.hsAttributesIndices.get(att.name());
    }

    /**
     * Completes the hashset with attributes indices.
     */
    private void computeAttributesIndices() {
        this.hsAttributesIndices = new HashMap<String, Integer>();
        // iterates through all existing attributes
        // and sets an unique identifier for each one of them
        for (int i = 0; i < this.numAttributes(); i++) {
            hsAttributesIndices.put(this.attribute(i).name(), i);
        }
    }
}
