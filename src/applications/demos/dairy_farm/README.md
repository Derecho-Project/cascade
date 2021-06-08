The Dairy Farm demo shows how to build a cattle health survilliance application using Cascade. A typical dairy farm milks the dairy cows twice a day using a voluntary milking system (VMS), where a camera installed at the entry of VMS captures every cow that walk in. The camera streams the video to a data center that will identify each cow and give a health score. This is achieved by two CNN models: a cow identification model and a health evaluation model. Please note that the live video from the camera is generally protected and preprocessed in the edge cloud. Only filtered frames are sent to a central cloud system. Here we use a simplified scenario to focus on the data center part.

**Application components**

object pools:
- /dairy_farm/front_end (TCSS)
- /dairy_farm/compute (VCSS)
- /dairy_farm/storage (PCSS)

DPLs:
- filter_dpl: "/dairy_farm/front_end" --> "/dairy_farm/compute"
- compute_dpl: "/dairy_farm/compute" --> "dairy_farm/storage"
