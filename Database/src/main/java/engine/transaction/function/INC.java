package engine.transaction.function;

public class INC extends Function {

    public INC(long delta) {

        this.delta = delta;
    }

    public INC(long delta, Condition condition) {
        this.delta = delta;
    }
}
