# Benchmarking
Benchmarks can be defined in the following format:
```json
{
  "benchmarks": [
    {
      "relations": [
        "relation1",
        "relation2",
        "relation3",
        "..."
      ],
      "sizes": [
        sz1,
        sz2,
        sz3,
        ...
      ],
      "join_attributes": [
        [
          "r1_key1", "r1_key2", ...
        ],
        [
          "r2_key1", "r2_key2", ...
        ],
        [
          "", "r3_key2", ...
        ],
        ...
      ]
    },
    ...
  ]
}
```
`relations` defines the participating relations and `sizes` defines how many tuples of that relation should be used. `join_attributes` holds as many arrays as there are relations. Each of those arrays holds as many strings as there are attributes that should be joined over. Each column then represents one join attribute. So in this example we join over `key1` which is present in `relation1` as `r1_key1` and in `relation2` as `r2_key1`. Equivalently we also join over `key2` which is present in `relation1` as `r1_key2`, in `relation2` as `r2_key2` and in `relation3` as `r3_key2`.

Our code tests the benchmarks specified in the `benchmarks.json` file. We are doing our benchmarking on 5 fixed relations that are defined in `benchmarks0.json` to `benchmarks4.json`. If you want to test those they have to be copied into `benchmarks.json`. Multiple benchmarks can be tested in one execution but with large benchmarks this might lead to the program being killed.

In `timing_params.json` we define the number of iterations for the measurements.