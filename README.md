# ceph-utils
collection of JTEK utilities used to maintain a Ceph cluster

* btrfs-defrag-scheduler.rb: defragmenter tuned for 7200rpm hard-drives,
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

The latest versions are able to handle more standard Btrfs usages with many
snapshots for example. It won't try to defragment read-only snapshots, will
consider all mounted subvolume as part of the main filesystem (its top volume
must be mounted somewhere to be considered). You must mount the filesystem
without autodefrag (or remount it with '-o remount,noautodefrag') or it will
be ignored until you do so.
Placing a '.no-defrag' file on the top volume root with one relative directory
(without a leading '/) will instruct it to ignore all files below in these
subtrees *including* all mountpoints of subvolumes included in these subtrees.
Use this file to avoid defragmenting data when competition for defragmentation
between several read-write copies of the same files exist between different
subvolumes (typically read-write snapshots with low usage).

When defragmenting, zlib is used to compress data if any compression is used
for the filesystem mount (this is expected to give better performance when
reading data back). 

Dependencies:
* Ruby 2.0 or later
* filefrag from e2fsprogs
* fatrace v0.10+
* btrfs-progs

Scrub scheduler
---------------

This script is used to avoid scrub or deep-scrub storms. It's setup to run a
deep-scrub every week and a scrub every 24h on every PG.

WARNING: without modification when an inconsistency is detected it triggers an
automatic repair (you probably want to change the email address it uses to
notify the admin when it happens). It works well on Btrfs (which detects
corruption)  but it wasn't tested on other filesystems where corrupted data
from the primary OSD could be copied to the secondary OSDs (Btrfs won't allow
corrupted data to be read forcing Ceph to get valid data from another OSD).

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
