package applications.spout.generator;

import java.util.Random;

public abstract class Generator<T> {

    final Random rand = new Random();

    public abstract T next();
}
