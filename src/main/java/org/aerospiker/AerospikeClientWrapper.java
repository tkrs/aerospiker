package org.aerospiker;
public final class AerospikeClientWrapper {
    final private com.aerospike.client.AerospikeClient client;
    public AerospikeClientWrapper(com.aerospike.client.AerospikeClient client) {
        this.client = client;
    }
    public <R> R execute(
            Class<R> clazz,
            com.aerospike.client.policy.WritePolicy policy,
            com.aerospike.client.Key key,
            String packageName,
            String functionName,
            com.aerospike.client.Value...args) {
        return (R) client.execute(policy, key, packageName, functionName, args);
    }
}
