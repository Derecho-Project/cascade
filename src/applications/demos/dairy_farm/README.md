TODO: add dairy farm description.

object pools:
- /dairy_farm/front_end (TCSS)
- /dairy_farm/compute (VCSS)
- /dairy_farm/storage (PCSS)

DPLs:
- filter_dpl: "/dairy_farm/front_end" --> "/dairy_farm/compute"
- compute_dpl: "/dairy_farm/compute" --> "dairy_farm/storage"
