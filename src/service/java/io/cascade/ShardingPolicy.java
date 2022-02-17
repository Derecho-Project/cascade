package io.cascade;

/**
 * An enum on the type of services offered by cascade.
 */
public enum ShardingPolicy {
    Hash(0),
    Range(1);

    private int value;

    private ShardingPolicy(int value) {
        this.value = value;
    }

    /**
     * Get the integer value corresponding to the enum.
     * 
     * @return the integer value corresponding to the enum.
     */
    public int getValue() {
        return value;
    }
}
