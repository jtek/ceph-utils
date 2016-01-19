# ceph-utils
collection of JTEK utilities used to maintain a Ceph cluster

* btrfs-defrag-scheduler.rb: defragmenter tuned for 7200rpm hard-drives and no snapshots, auto-detects Ceph OSD,
* scrub-scheduler.rb: schedule scrubs and deep-scrubs trying to avoid IO peaks on OSDs.
