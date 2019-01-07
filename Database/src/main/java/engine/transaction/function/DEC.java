package engine.transaction.function;

public class DEC extends Function {

    public DEC(long delta) {

        this.delta = delta;
    }

    public DEC(long delta, Condition condition) {
        this.delta = delta;
    }
}
