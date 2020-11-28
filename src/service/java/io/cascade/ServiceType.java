package io.cascade;

/**
 * An enum on the type of services offered by cascade.
 */
public enum ServiceType {
    // VCSU: volatile cascade store with uint64 keys
    // PCSU: persistent cascade store with uint64 keys
    // VCSS: volatile cascade store with string keys
    // PCSS: persistent cascade store with string keys
    VCSU(0), PCSU(1), VCSS(2), PCSS(3);

    private int value;

    private ServiceType(int value) {
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