file(REMOVE_RECURSE
  "libcascade.pdb"
  "libcascade.so"
  "libcascade.so.1.0rc0"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/cascade.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
