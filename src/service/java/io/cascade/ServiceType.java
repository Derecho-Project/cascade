package io.cascade;

/**
 * An enum on the type of services offered by cascade.
 */
public enum ServiceType {
    VolatileCascadeStoreWithStringKey(0), 
    PersistentCascadeStoreWithStringKey(1),
    TriggerCascadeNoStoreWithStringKey(2);

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
