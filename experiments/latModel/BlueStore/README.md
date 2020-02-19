## The experiment in Ceph with BlueStore

`base-batch-1`:
1. CoDel is disabled
2. committing batch in BlueStore is set to 1
3. except `dump-codel-tests.csv`, all other data are raw data
4. `dump-codel-tests.csv` is generated in BlueStore. The first and last 5 data points are removed for steady state. This file is used in the plot script. 
5. The data is getting from my branch: https://github.com/yzhan298/ceph.git, research\_rs\_mg
6. In order to dump these csvs, you need to call ceph admin socket commnad: `bin/ceph daemon osd.0 dump kvq vector` 
