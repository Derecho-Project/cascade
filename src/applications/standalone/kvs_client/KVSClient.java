import io.cascade.*;

public class KVSClient {
    public static final void main(String[] args) {
        /**
         *
         */
         Client client = new Client();
         System.out.println(client.getMembers());
         System.out.println(client.getShardMembers(ServiceType.VolatileCascadeStoreWithStringKey, 0, 0));
         System.out.println(client.getShardMembers(ServiceType.PersistentCascadeStoreWithStringKey, 0, 0));
         System.out.println(client.getShardMembers(ServiceType.TriggerCascadeNoStoreWithStringKey, 0, 0));
    }
}
