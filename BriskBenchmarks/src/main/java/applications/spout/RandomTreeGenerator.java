package applications.spout;

public class RandomTreeGenerator {
    public RandomTreeGenerator(int thisTaskId, int numContestants) {

    }

    public long nextInstance() {
        return 0;
    }


    protected void generateHeader() {
//        FastVector<Attribute> attributes = new FastVector<>();
//        FastVector<String> nominalAttVals = new FastVector<>();
//        for (int i = 0; i < this.numValsPerNominalOption.getValue(); i++) {
//            nominalAttVals.addElement("value" + (i + 1));
//        }
//        for (int i = 0; i < this.numNominalsOption.getValue(); i++) {
//            attributes.addElement(new Attribute("nominal" + (i + 1),
//                    nominalAttVals));
//        }
//        for (int i = 0; i < this.numNumericsOption.getValue(); i++) {
//            attributes.addElement(new Attribute("numeric" + (i + 1)));
//        }
//        FastVector<String> classLabels = new FastVector<>();
//        for (int i = 0; i < this.numClassesOption.getValue(); i++) {
//            classLabels.addElement("class" + (i + 1));
//        }
//        attributes.addElement(new Attribute("class", classLabels));
//        this.streamHeader = new InstancesHeader(new Instances(
//                getCLICreationString(InstanceStream.class), attributes, 0));
//        this.streamHeader.setClassIndex(this.streamHeader.numAttributes() - 1);
    }

    public void prepareForUse() {

        //generate instance header.



    }
}
