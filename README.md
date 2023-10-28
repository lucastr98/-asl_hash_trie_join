# Advanced Systems Lab - Optimizing Worst-Case Optimal Joins
In a team of four we implemented and optimized the algorithms described in the paper [Combining Worst-Case Optimal and Traditional Binary Join Processing](https://db.in.tum.de/~freitag/papers/p1891-freitag.pdf). I mainly focused on algorithms 2 & 3 and therefore only kept that part in this repo. For more details about the optimizations have a look at the report (`./report.pdf`).

## Build
### Hash Trie Join 
To build the project using `cmake` you need to create a build folder locally (it will be gitignored). Then you need to go into the build folder and execute the following two commands:
```
cmake ..
make
```
This should generate an executable called `hash_trie_join_program` inside the build folder which can be executed with:
```
./hash_trie_join_program
```

## Sql setup

Make sure you have postgreSQL installed (https://www.postgresql.org/download/).

In case of the CMake error "could NOT find PostgreSQL", ensure that you have both libpq-dev and postgresql-server-dev installed:
```
$ dpkg --get-selections | grep -e "libpq-dev\|postgresql-server-dev"
```
In case you're missing something, do:
```
$ apt-get install libpq-dev postgresql-server-dev-all
```

* Check this repo for info on getting TPC-H data into csv: https://github.com/tvondra/pg_tpch, then place into `./sql/data`.
* Make sure that the files in `./sql/data` are in csv format.
* Create a PostgreSQL database by the name of tpch_db.
* run `setup_database.sh`
```
sh ./setup_database.sh <DB_USER> <DB_PASSWORD>
```
