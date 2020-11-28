package io.cascade;

/**
 * An enum on the type of shard member selection policies offered by cascade.
 */
public enum ShardMemberSelectionPolicy {

    FirstMember(0), // use the first member in the list returned from get_shard_members(), this is
                    // the default behaviour.
    LastMember(1), // use the last member in the list returned from get_shard_members()
    Random(2), // use a random member in the shard for each
               // operations(put/remove/get/get_by_time).
    FixedRandom(3), // use a random member and stick to that for the following operations.
    RoundRobin(4), // use a member in round-robin order.
    UserSpecified(5), // user specify which member to contact.
    InvalidPolicy(-1);

    private int value;
    private int unode = -1; // user should set this value if their policy is user specified.

    private ShardMemberSelectionPolicy(int value) {
        this.value = value;
    }

    /**
     * Get the value corresponding to the policy.
     */
    public int getValue() {
        return value;
    }

    /**
     * Get the user specified node if the policy is UserSpecified.
     */
    public int getUNode() {
        return unode;
    }

    /**
     * Set the user specified node if the policy is UserSpecified.
     */
    public void setUNode(int u) {
        unode = u;
    }

}