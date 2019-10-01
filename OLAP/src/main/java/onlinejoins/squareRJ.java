package onlinejoins;

/**
 * Ripple join can be viewed as a generalization of nested-loops
 * join in which the traditional roles of “inner” and “outer”
 * relation are continually interchanged during processing.
 * <p>
 * This is a simple square two-table ripple join.
 */
public class squareRJ {

    Record[] R;
    Record[] S;

    int size_R;
    int size_S;

    public squareRJ(int size_R, int size_S) {
        this.size_R = size_R;
        this.size_S = size_S;
        assert this.size_R <= this.size_S; //size_R <= size_S
    }

    public void join() {

        for (int i = 0; i < size_S; i++) {//for every step of look up S, look up an array of R.
            for (int j = 0; j < i - 1; j++) {
                if (predicate(R[j], S[i])) {
                    output(R[j], S[i]);
                }
            }

            for (int j = 0; j < i; i++) {
                if (predicate(R[i], S[j])) {
                    output(R[i], S[j]);
                }
            }

        }
    }

    private void output(Record r, Record s) {

    }

    private boolean predicate(Record r, Record s) {
        return false;
    }

}
