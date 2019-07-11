package applications.ml.datatypes.instance;


import applications.ml.datatypes.Attribute;
import applications.ml.datatypes.AttributesInformation;
import applications.ml.datatypes.Range;

import java.util.List;

/**
 * training data instance information
 */
public class InstanceInformation {

    /**
     * The dataset's name.
     */
    protected String relationName;

    protected AttributesInformation attributesInformation;

    /**
     * The class index.
     */
    protected int classIndex = Integer.MAX_VALUE; //By default is multilabel

    /**
     * Range for multi-label instances.
     */
    protected Range range;

    public Attribute inputAttribute(int w) {
        return this.attributesInformation.attribute(inputAttributeIndex(w));
    }

    public Attribute outputAttribute(int w) {
        return this.attributesInformation.attribute(outputAttributeIndex(w));
    }

    /**
     * Instantiates a new instance information.
     *
     * @param chunk the chunk
     */
    public InstanceInformation(InstanceInformation chunk) {
        this.relationName = chunk.relationName;
        this.attributesInformation = chunk.attributesInformation;
        this.classIndex = chunk.classIndex;
    }

    /**
     * Instantiates a new instance information.
     *
     * @param st the st
     * @param v the v
     */
    public InstanceInformation(String st, List<Attribute> input) {
        this.relationName = st;
        this.attributesInformation = new AttributesInformation(input, input.size());
    }

    /**
     * Instantiates a new instance information.
     */
    public InstanceInformation() {
        this.relationName = null;
        this.attributesInformation = null;
    }

    //Information Instances
    /* (non-Javadoc)
     * @see com.yahoo.labs.samoa.instances.InstanceInformationInterface#setRelationName(java.lang.String)
     */
    public void setRelationName(String string) {
        this.relationName = string;
    }

    /* (non-Javadoc)
     * @see com.yahoo.labs.samoa.instances.InstanceInformationInterface#getRelationName()
     */
    public String getRelationName() {
        return this.relationName;
    }

    /* (non-Javadoc)
     * @see com.yahoo.labs.samoa.instances.InstanceInformationInterface#classIndex()
     */
    public int classIndex() {
        return this.classIndex;
    }

    /* (non-Javadoc)
     * @see com.yahoo.labs.samoa.instances.InstanceInformationInterface#setClassIndex(int)
     */
    public void setClassIndex(int classIndex) {
        this.classIndex = classIndex;
    }

    /* (non-Javadoc)
     * @see com.yahoo.labs.samoa.instances.InstanceInformationInterface#classAttribute()
     */
    public Attribute classAttribute() {
        return this.attribute(this.classIndex());
    }

    /* (non-Javadoc)
     * @see com.yahoo.labs.samoa.instances.InstanceInformationInterface#numAttributes()
     */
    public int numAttributes() {
        return this.attributesInformation.numberAttributes;
    }

    /* (non-Javadoc)
     * @see com.yahoo.labs.samoa.instances.InstanceInformationInterface#attribute(int)
     */
    public Attribute attribute(int w) {
        return this.attributesInformation.attribute(w);
    }

    /* (non-Javadoc)
     * @see com.yahoo.labs.samoa.instances.InstanceInformationInterface#numClasses()
     */
    public int numClasses() {
        return this.attributesInformation.attribute(classIndex()).numValues();
    }

    /* (non-Javadoc)
     * @see com.yahoo.labs.samoa.instances.InstanceInformationInterface#deleteAttributeAt(java.lang.Integer)
     */
    public void deleteAttributeAt(Integer integer) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /* (non-Javadoc)
     * @see com.yahoo.labs.samoa.instances.InstanceInformationInterface#insertAttributeAt(com.yahoo.labs.samoa.instances.Attribute, int)
     */
    public void insertAttributeAt(Attribute attribute, int i) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void setAttributes(List<Attribute> v) {
        if(this.attributesInformation==null)
            this.attributesInformation= new AttributesInformation();
        this.attributesInformation.setAttributes(v);
    }

    public int inputAttributeIndex(int index) {
        int ret = 0;
        if (classIndex == Integer.MAX_VALUE) {//Multi Label
            if(index<range.getStart())//JD
                ret= index;
            else
                ret= index+range.getSelectionLength();

        } else { //Single Label
            ret = classIndex() > index ? index : index + 1;
        }
        return ret;
    }

    public int outputAttributeIndex(int attributeIndex) {
        int ret = 0;
        if (classIndex == Integer.MAX_VALUE) {//Multi Label
            ret=attributeIndex+range.getStart(); //JD - Range should be a "block"
        } else { //Single Label
            ret = classIndex;
        }
        return ret;
    }

    public int numInputAttributes() {
        int ret = 0;
        if (classIndex == Integer.MAX_VALUE) {//Multi Label
            ret=this.numAttributes()-range.getSelectionLength(); //JD
        } else { //Single Label
            ret = this.numAttributes() - 1;
        }
        return ret;
    }

    public int numOutputAttributes() {
        int ret = 0;
        if (classIndex == Integer.MAX_VALUE) {//Multi Label
            ret=range.getSelectionLength(); //JD
        } else { //Single Label
            ret = 1;
        }
        return ret;
    }

    public void setRangeOutputIndices(Range range) {
        this.setClassIndex(Integer.MAX_VALUE);
        this.range = range;
    }

    public void setAttributes(List<Attribute> v, List<Integer> indexValues) {
        if(this.attributesInformation==null)
            this.attributesInformation= new AttributesInformation();
        this.attributesInformation.setAttributes(v,indexValues);

    }

}
