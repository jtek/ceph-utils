# ceph-utils
collection of JTEK utilities used to maintain a Ceph cluster

* btrfs-defrag-scheduler.rb: defragmenter tuned for 7200rpm hard-drives and no
  snapshots, auto-detects Ceph OSD,
* scrub-scheduler.rb: schedule scrubs and deep-scrubs trying to avoid IO peaks
  on OSDs.

Defragmentation scheduler
-------------------------

By default it stores its state in the /root/.btrfs_defrag directory which it
creates automatically.

The scheduler both tracks file writes to detect recent heavy fragmentation and
slowly scans the whole filesystem over a one week period (the number of hours
targeted for this slow scan can be passed as a parameter). During the slow
scan it detects lower fragmentation and fragmentation it didn't have time to
remove yet.

The scheduler tries to restrict IO load and memory usage in several ways. With
default settings it should not put any significant load on a Ceph OSD and
should not use significantly more than 100MB by OSD.

When defragmenting, zlib is used to compress data if any compression is used
for the filesystem mount (this is expected to give better performance when
reading data back). The defragmentation settings are optimized for 4MB files
too.

Dependencies:
* Ruby 2.0 or later
* filefrag from e2fsprogs
* fatrace v0.10
* btrfs-progs

Scrub scheduler
---------------

This script is used to avoid scrub or deep-scrub storms. It's setup to run a
deep-scrub every week and a scrub every 24h on every PG.

If possible the script will try to avoid scrubbing PGs using the same OSD
several times in a row, offsetting the scrubs a bit in the future to
give priority to PGs on OSDs without recent scrub activity. With small enough
PG sizes this should avoid removing too much useful data from disk caches.

To avoid any storm the scrub intervals should be setup in ceph.conf to target
longer periods. For example:

    osd scrub min interval       = 172800 # 60*60*24*2
    osd scrub max interval       = 259200 # 60*60*24*3
    osd deep scrub interval      = 1209600 # 60*60*24*14

Dependencies:
* Ruby 2.0 or later
* ceph command
