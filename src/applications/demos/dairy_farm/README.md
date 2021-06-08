The Dairy Farm demo shows how to build a cattle health surveillance application using Cascade. A typical dairy farm milks the dairy cows twice a day using a voluntary milking system (VMS), where a camera installed at the entry of VMS captures the images of every cow that walks in. The camera streams the video to a data center that will identify each cow and give a health score. Our demo application incorporates three ML models to achieve this goal. A filter model detects whether a video frame contains a cow or not; a cow identification model identifies the cow in a frame; a health evaluation model gives a health score based on the cow image. Please note that the live video from the camera is generally protected and preprocessed in the edge cloud. The edge cloud sends filtered frames to a central cloud system. Here we use a simplified scenario to focus on the data center part.

# Prerequisite Concepts
Cascade organizes the objects in a file-system approach. The *objects* in a directory are of the same type and for the same purpose. An *object pool* is a container for all objects included in a directory and its subdirectories. The object pool metadata is placement information like the subgroup to store the objects and the sharding policy. *Cascade metadata service* manages that information for all object pools in the system. When a client access objects in an object pool, it retrieves and caches the object pool metadata to find the corresponding cascade server node. 

Please note that object pools do not nest.
If an object pool `/A/B/` exists, Cascade will refuse the creation requests for object pools like `/A/B/C/` or `/A/`. That is because object pool `/A/B/` has enforced the placement of the objects in `/A/B/` already. Allowing new placement information in `/A/` or `/A/B/C/` will cause ambiguity.

*User Defined Logic* or UDL is a codelet (binary or scripts) developed by an application programmer. A UDL can be registered to one or multiple directories in the Cascade file system. Once registered, putting an object in that directory or its subdirectories will trigger the registered UDL.

*Data Flow Graph* or DFG is a configuration file in JSON format. It specifies which UDLs are registered to which directories and where the output of the UDLs should go.

# Application components
<table>
  <tr>
    <td><img src="dairy_farm_arch.pdf"></td>
  </tr>
  <tr>
    <td>Dairy Farm Demo Architecture</td>
  </tr>
</table>
The above figure shows the architecture of this demo. The core of this architecture consists of three object pools. The `/dairy_farm/front_end` object pool locates in a transient subgroup (`TriggerCascadeNoStore<>`) because it does not store data. We attached a filter UDL to the `/diary_farm/front_end` object pool, which calls the filter model to detect if a cow is possibly in a video frame. If positive, the filter UDL relay the frame object to the computation tier, the `/dairy_farm/compute` object pool. The `/dairy_farm/compute` object pool locates in a volatile subgroup (`VolatileCascadeStore<>`). We attach a computation UDL to the `/dairy_farm/compute` object pool, which calls the cow identification model and a health evaluation model to identify the cow id and give a health score. The computation UDL sends the cow id and health score pair to the storage tier, the `/dairy_farm/storage` object pool, which stores the records in a persisted log.

Each of the object pools is mapped to subgroups sharded for performance and replicated for fault tolerance. Please check the configuration file to see how to configure the subgroup layout.

object pools:
- /dairy_farm/front_end (TCSS)
- /dairy_farm/compute (VCSS)
- /dairy_farm/storage (PCSS)

DPLs:
- filter_dpl: "/dairy_farm/front_end" --> "/dairy_farm/compute"
- compute_dpl: "/dairy_farm/compute" --> "dairy_farm/storage"
