package io.cascade;

/**
 * An enum on the type of services offered by cascade.
 */
public enum ServiceType {
    // VCSS: volatile cascade store with string keys
    // PCSS: persistent cascade store with string keys
    VCSS(0), PCSS(1);

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
