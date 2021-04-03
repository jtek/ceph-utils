#!/usr/bin/ruby

require 'getoptlong'
require 'set'
require 'digest'

def help_exit
  script_name = File.basename($0)
  print <<EOMSG
Usage: #{script_name} [ options ]

Recognized options:

--help (-h)
    This message

--full-scan-time <value> (-s)
    Number of hours over which to scan the filesystem (>= 0.05)
    values < 1 only meaningful for developpers
    default: 4 x 7 x 24 (4 weeks)

--target-extent-size <value> (-e)
    value passed to btrfs filesystem defrag "-t" parameter
    default: 32M

--trees (-f)
     fully defragment the read/write subvolume by launching an extent
     and subvolume trees defragment after each full scan
     WARNING: IO performance on HDD suffers greatly on large filesystems
     default: disabled

--verbose (-v)
    prints defragmention as it happens

--debug (-d)
    prints internal processes information

--speed-multiplier <value> (-m)
    slows down (<1.0) or speeds up (>1.0) the defragmentation process
    the process try to avoid monopolizing the I/O bandwidth,
    don't increase this unless you have to correct the default behaviour
    default: 1.0

--slow-start <value> (-l)
    wait for <value> seconds before scanning the filesystems,
    files modified/created are still processed during this period
    default: 600

--drive-count <value> (-c)
    number of 7200rpm drives used by the filesystem
    this changes the cost of seeking when computing fragmentation costs
    more drives: less cost
    default: 1

--ignore-load (-i)
    WARNING: this can and will reduce your FS performance during heavy I/O
    by default the scheduler slows down if the system load is greater than the
    number of processors it detects (warning: setting CPU affinity for the
    scheduler will artificially reduce this number).
    default: off

--target-load (-t)
    load to use as a threshold for reducing defragmentation activity
    use if the default load target isn't ideal
    default: number of CPUs detected
EOMSG
  exit
end

opts =
  GetoptLong.new([ '--help', '-h', '-?', GetoptLong::NO_ARGUMENT ],
                 [ '--verbose', '-v', GetoptLong::NO_ARGUMENT ],
                 [ '--debug', '-d', GetoptLong::NO_ARGUMENT ],
                 [ '--ignore-load', '-i', GetoptLong::NO_ARGUMENT ],
                 [ '--full-scan-time', '-s', GetoptLong::REQUIRED_ARGUMENT ],
                 [ '--trees', '-f', GetoptLong::NO_ARGUMENT ],
                 [ '--target-extent-size', '-e',
                   GetoptLong::REQUIRED_ARGUMENT ],
                 [ '--speed-multiplier', '-m', GetoptLong::REQUIRED_ARGUMENT ],
                 [ '--slow-start', '-l', GetoptLong::REQUIRED_ARGUMENT ],
                 [ '--drive-count', '-c', GetoptLong::REQUIRED_ARGUMENT ],
                 [ '--target-load', '-t', GetoptLong::REQUIRED_ARGUMENT ])

# Latest recommendation from BTRFS developpers as of 2016
$default_extent_size = '32M'
$defragment_trees = false
$verbose = false
$debug = false
$ignore_load = false
$target_load = nil
$speed_multiplier = 1.0
$drive_count = 1
slow_start = 600
scan_time = nil
opts.each do |opt,arg|
  case opt
  when '--help'
    help_exit
  when '--verbose'
    $verbose = true
  when '--debug'
    $debug = true
  when '--full-scan-time'
    scan_time = arg.to_f
    help_exit if scan_time < 0.05
  when '--trees'
    $defragment_trees = true
  when '--target-extent-size'
    $default_extent_size = arg
  when '--speed-multiplier'
    $speed_multiplier = arg.to_f
    $speed_multiplier = 1.0 if $speed_multiplier <= 0
  when '--slow-start'
    slow_start = arg.to_i
    slow_start = 600 if slow_start < 0
  when '--drive-count'
    $drive_count = arg.to_f
    $drive_count = 1 if $drive_count < 1
  when '--ignore_load'
    $ignore_load = true
  when '--target_load'
    $target_load = arg.to_f
  end
end


# This defragments Btrfs filesystem files
# the whole FS is scanned over SLOW_SCAN_PERIOD
# meanwhile fatrace is used to monitor written files
# if these are heavily fragmented, defragmentation is triggered early

# Used to remember how low the cost is brought down
# higher values means more stable behaviour in the effort made
# to defragment
COST_HISTORY_SIZE = 500
COST_HISTORY_TTL = 7200
# Tune this to change the effort made to defragment (1.0: max effort)
# this is a compromise: some files are written to regularly, you don't want to
# defragment them as soon as they begin to fragment themselves or you will have
# to defragment them very often, generating I/O load that would defeat the
# purpose of defragmenting (keeping latencies low)
MIN_FRAGMENTATION_THRESHOLD = 1.05
# Some files can't be defragmented below 1.05, especially when BTRFS uses
# compression
# So we track the level of fragmentation obtained after defragmenting to detect
# the level where we should stop trying
# Note: asking for defragmenting files that won't be may not generate disk load
# at least no disk writes (and reads have high cache hits probabilities)
COST_THRESHOLD_PERCENTILE = 25
# Defragmentation rate that must have happened recently to trust the threshold
COST_THRESHOLD_TRUST_LEVEL = 20.0 / 3600 # 20 per hour
COST_THRESHOLD_TRUST_PERIOD = 1800       # 30 minutes
COST_COMPUTE_DELAY = 60
HISTORY_SERIALIZE_DELAY = 3600
RECENT_SERIALIZE_DELAY = 120
FILECOUNT_SERIALIZE_DELAY = 300

# How many files do we queue for defragmentation
MAX_QUEUE_LENGTH = 2000
# What is our target queue length proportion where our defragmentation rate
# and scan rate are nominal
# speed up defragmentation and slow down scan above it and inverse under it
QUEUE_PROPORTION_EQUILIBRIUM = 0.05
# How much device time the program is allowed to use (including filefrag calls)
# time window => max_device_use_ratio
# the allowed use_ratio is reduced when:
# - system load exceeds target (to avoid avalanche effects)
# - the defrag queue is near empty (to avoid fast successions of defrag when
#   possible)
DEVICE_USE_LIMITS = {
  0.2  => 0.75 * $speed_multiplier,
  0.5  => 0.6 * $speed_multiplier,
  3.0  => 0.5 * $speed_multiplier,
  11.0 => 0.45 * $speed_multiplier,
  30.0 => 0.4 * $speed_multiplier
}
DEVICE_LOAD_WINDOW = DEVICE_USE_LIMITS.keys.max
EXPECTED_COMPRESS_RATIO = 0.5

# How many files do we track for writes, we pass these to the defragmentation
# queue when activity stops (this amount limits memory usage)
MAX_TRACKED_WRITTEN_FILES = 20_000
# Period over which to distribute defragmentation checks for files which
# were written at the same time, this avoids filefrag call storms
DEFRAG_CHECK_DISTRIBUTION_PERIOD = 120
# How often do we check fragmentation of files written to
TRACKED_WRITTEN_FILES_CONSOLIDATION_PERIOD = 5
# Default Btrfs commit delay when none is specified
# it's otherwise parsed from /proc/mounts
DEFAULT_COMMIT_DELAY = 30
# How often do we check for defragmentation progress
STAT_QUEUE_INTERVAL = 10
# Fragmentation information isn't available right after the last write or even
# commit (as in commit_delay of the filesystem)
FRAGMENTATION_INFO_DELAY_FACTOR = 4
# Some files might be written to constantly, don't delay passing them to
# filefrag more than that
MAX_WRITES_DELAY = 4 * 3600

# Full refresh of fragmentation information on files happens in
# (pass number of hours on commandline if the default is not optimal for you)
SLOW_SCAN_PERIOD = (scan_time || 4 * 7 * 24) * 3600 # 1 month
SLOW_SCAN_CATCHUP_WAIT = slow_start
# If the scan didn't finish after the initial period by how much do we speed up
# this will be repeated until reaching max speed SLOW_SCAN_PERIOD / 2 after
# the initial period
SLOW_SCAN_SPEED_INCREASE_STEP = 1.1

# These are used to compensate for deviation of the slow scan progress
SLOW_SCAN_MAX_SPEED_FACTOR = 1.5
SLOW_SCAN_MIN_SPEED_FACTOR = 0.02
# Sleep constraints between 2 filefrags call in full refresh thread
MIN_DELAY_BETWEEN_FILEFRAGS = 0.1 / $speed_multiplier
MAX_DELAY_BETWEEN_FILEFRAGS = 120 / $speed_multiplier
# Batch size constraints for full refresh thread
# don't make it so large that at cruising speed it could overflow the queue
# with only one batch
MAX_FILES_BATCH_SIZE = MAX_QUEUE_LENGTH / 4
MIN_FILES_BATCH_SIZE = 1

# We ignore files recently defragmented for 12 hours
IGNORE_AFTER_DEFRAG_DELAY = 12 * 3600

#MIN_DELAY_BETWEEN_DEFRAGS = 0.05
# Actually max delay before checking when to defrag next
#MAX_DELAY_BETWEEN_DEFRAGS = 15
MIN_QUEUE_DEFRAG_SPEED_FACTOR = 0.2

# How often do we dump a status update
STATUS_PERIOD = 120 # every 2 minutes
SLOW_STATUS_PERIOD = 1800 # every 30 minutes
# How often do we check for new filesystems or umounted filesystems
FS_DETECT_PERIOD = 60
# How often do we restart the fatrace thread ?
# there were bugs where fatrace would stop reporting modifications under
# some conditions (mounts or remounts, fatrace processes per mountpoint and
# old fatrace version), it might not apply anymore but this doesn't put any
# measurable load on the system and we are unlikely to miss files
FATRACE_TTL = 24 * 3600 # every day
# How often do we check the subvolumes list ?
# it can be costly but we need them to avoid defragmenting read-only snapshots
# (we consider these not useful to defragment)
SUBVOL_TTL = 3600

# System dependent (reserve 100 for cmd and 4096 for one path entry)
FILEFRAG_ARG_MAX = 131072 - 100 - 4096
FILEFRAG_ARGCOUNT_MAX = 25

# Where do we serialize our data
STORE_DIR        = "/root/.btrfs_defrag"
FILE_COUNT_STORE = "#{STORE_DIR}/filecounts.yml"
HISTORY_STORE    = "#{STORE_DIR}/costs.yml"
RECENT_STORE     = "#{STORE_DIR}/recent.yml"

# Per filesystem defrag blacklist
DEFRAG_BLACKLIST_FILE = ".no_defrag"

$output_mutex = Mutex.new

# Synchronize multithreaded outputs
module Outputs
  def short_tstamp
    Time.now.strftime("%Y%m%d %H%M%S")
  end
  def error(msg)
    $output_mutex.synchronize { STDERR.puts "#{short_tstamp}: ERROR, #{msg}" }
  end
  def info(msg)
    $output_mutex.synchronize { STDOUT.puts "#{short_tstamp}: #{msg}" }
  end
  def print(msg)
    $output_mutex.synchronize { STDOUT.print "#{short_tstamp}: #{msg}" }
  end
end

require 'find'
require 'pathname'
require 'yaml'
require 'fileutils'

Thread.abort_on_exception = true

# Shared code for classes needing a storage
# note: write isn't safe, so we protect reads with rescue
# Idea: centralize writes from all writers and optionnaly wait to commit
# instead of commiting for every write
module HashEntrySerializer
  def serialize_entry(key, value, file)
    FileUtils.mkdir_p(File.dirname(file))
    File.open(file, File::RDWR|File::CREAT|File::BINARY, 0644) { |f|
      f.flock(File::LOCK_EX)
      yaml = f.read
      hash = yaml.empty? ? {} : YAML.load(yaml) rescue {}
      hash[key] = value
      f.rewind
      f.write(YAML.dump(hash))
      f.flush
      f.truncate(f.pos)
    }
  end

  def unserialize_entry(key, file, op_id, default_value = nil)
    if File.file?(file)
      File.open(file, 'r:ascii-8bit') { |f|
        f.flock(File::LOCK_EX)
        yaml = f.read
        hash = yaml.empty? ? {} : YAML.load(yaml) rescue {}
        info("= #{key}, #{op_id}: %sloaded" %
             (hash[key] ? "" : "default "))
        hash[key] || default_value
      }
    else
      info "= #{key}, #{op_id}: default loaded"
      default_value
    end
  end
end

module HumanFormat
  def human_delay_since(timestamp)
    return "unknown" unless timestamp

    delay = Time.now - timestamp
    human_duration(delay)
  end

  def human_duration(duration)
    delay_maps = { 24 * 3600 => "d",
                   3600      => "h",
                   60        => "m",
                   1         => "s",
                   nil       => "now" }
    delay_maps.each do |amount, suffix|
      return suffix unless amount
      return "%.1f%s" % [ duration / amount, suffix ] if duration >= amount
    end
  end
end

# Thread-safe load checker (won't check load more than once per period
# accross all threads)
class LoadCheck
  require 'singleton'
  require 'etc'
  include Singleton

  # 10 seconds seems small enough to detect changes with real-life impacts
  LOAD_VALIDITY_PERIOD = 10

  def initialize
    @load_updater_mutex = Mutex.new
    @next_update = Time.now - 1
    update_load_if_needed
  end

  # We don't slow down until load_ratio > 1
  def slowdown_ratio
    return 1 if $ignore_load
    [ load_ratio, 1 ].max
  end

  def load_ratio
    update_load_if_needed
    # Note: not protected by mutex because Ruby concurrent access semantics
    # don't allow this object to have transient invalid values
    @load_ratio
  end

  private

  def update_load_if_needed
    return if Time.now <= @next_update
    @load_updater_mutex.synchronize { update_load }
  end

  def update_load
    # Warning Etc.nprocessors is restricted by CPU affinity
    @load_ratio = cpu_load / target_load
    @next_update = Time.now + LOAD_VALIDITY_PERIOD
  end

  def target_load
    return $target_load if $target_load
    Etc.nprocessors
  end

  def cpu_load
    File.read('/proc/loadavg').split(' ')[0].to_f
  end

end

# Limit disk available bandwidth usage
class UsagePolicyChecker
  def initialize(btrfs)
    @btrfs = btrfs
    @device_uses = []
    @mutex = Mutex.new
    @last_load_check = Time.now
    @average_load = load
  end

  # Caller should use delay_until_available before using this
  def run_with_device_usage(&block)
    # Prevent concurrent accesses
    start, result = @mutex.synchronize { [ Time.now, block.call ] }
    add_usage(start, Time.now)
    result
  end

  def delay_until_available(expected_time: 0, min_delay: 0, use_limit_factor: 1)
    [ available_at(expected_time: expected_time, min_delay: min_delay,
                   use_limit_factor: use_limit_factor) - Time.now, 0 ].max
  end

  # Might move the following to global constants/parameters later
  # Cap the maximum value (avoids a division by 0 and limit the slowdown)
  MAX_LOAD_FOR_SLOWDOWN = 0.95
  # Don't slow down for low loads
  LOAD_SLOWDOWN_LEVEL = 0.2
  # Store this to avoid computing it everytime
  CORRECTION_FACTOR = MAX_LOAD_FOR_SLOWDOWN / (1 - LOAD_SLOWDOWN_LEVEL)
  # Returns how much we can expect IO to be slowed down by
  # the IO bandwidth used based on recent usage to adapt the load
  # we create ourselves
  def expected_slowdown
    io_load = [ conservative_load - LOAD_SLOWDOWN_LEVEL, 0 ].max
    io_load *= CORRECTION_FACTOR
    1.0 / (1 - io_load)
  end

  # This is an approximation of the IO load generated by the scheduler
  def load
    activity = 0.0
    now = Time.now
    window_start = now - DEVICE_LOAD_WINDOW
    @device_uses.each do |start, stop|
      next if stop <= window_start
      activity += stop - [ start, window_start ].max
    end
    load = activity / DEVICE_LOAD_WINDOW
    if @last_load_check <= (now - (DEVICE_LOAD_WINDOW / 2))
      @average_load = (@average_load + load) / 2
      @last_load_check = now
    end
    load
  end

  private

  # If load is going down, prefere the average load
  def conservative_load
    [ load, @average_load ].max
  end

  def add_usage(start, stop)
    @device_uses << [ start, stop ]
  end

  def available_at(expected_time: 0, min_delay: 0, use_limit_factor:)
    cleanup
    DEVICE_USE_LIMITS.keys.map do |window|
      next_available_for(window, expected_time, min_delay, use_limit_factor)
    end.max
  end

  def cleanup
    # Adapt period to current load, keep a buffer to anticipate rise in load
    this_start = Time.now
    # Cleanup the device uses
    @device_uses.shift while (first = @device_uses.first) &&
                             first[1] < (this_start - DEVICE_LOAD_WINDOW)
  end

  def next_available_for(window, expected_time, min_delay, use_limit_factor)
    now = Time.now
    return now + min_delay if window <= min_delay
    use_factor = use_limit_factor / LoadCheck.instance.slowdown_ratio
    target = DEVICE_USE_LIMITS[window] * use_factor
    # When will it reach the target use_ratio ?
    return now + dichotomy((min_delay..window), target, 0.001) do |wait|
      use_ratio(now + wait - window, window, expected_time)
    end
  end

  # Return ratio without and with expected_time of next task
  def use_ratio(start, duration, expected_time)
    time_spent = 0
    @device_uses.each do |use_start, use_stop|
      next if use_stop < start
      time_spent += use_stop - [ use_start, start ].max
    end
    # We don't consider expected_time if there's no device_use
    return 0 if time_spent == 0
    # Anything else and we return a normal ratio
    max_time = [ start + duration - Time.now, expected_time ].min
    (time_spent + max_time) / duration
  end

  # Assumes passed block is a monotonic decreasing function
  # precision is used to limit the effort made to find the best value
  def dichotomy(range, target, precision)
    start = Time.now if $debug
    min = range.min.to_f; max = range.max.to_f
    return min if yield(min) < target
    return max if yield(max) > target
    steps = 0
    while (max - min) > precision
      steps += 1
      middle = (min + max) / 2
      if yield(middle) > target
        min = middle
      else
        max = middle
      end
    end
    # We return the max to avoid the possibility of waiting twice
    info "## dichotomy: %d steps, %.2fs, result: %.2fs" %
         [ steps, (Time.now - start), max ] if $debug
    max
  end
end

# Model for impact on fragmentation performance
# this emulates a typical 7200tpm SATA/SAS disk
# TODO: autodetect or allow configuring other latencies
module FragmentationCost
  # Disk modelization for computing fragmentation cost
  # A track should be ~1.25MB with current hardware according to
  # surface density and estimated platter surface
  DEVICE_CAPACITY = 2 * 1000 ** 4
  TRACK_SIZE = 1.25 * 1024 ** 2
  # Total number for the device (not per plater)
  TRACK_COUNT = DEVICE_CAPACITY / TRACK_SIZE
  TRACK_READ_DELAY = 1.0 / 120 # 7200t/mn
  TRANSFER_RATE = TRACK_SIZE / TRACK_READ_DELAY
  # Typical seek times for 7200t/mn drives
  MIN_SEEK = 0.002 # Track to track
  MAX_SEEK = 0.016 # Whole disk seek (based on 0.009 average)
  # Average time wasted looking for another track
  SEEK_DELAY = (MIN_SEEK + MAX_SEEK) / 2
  # BTRFS parameters
  BTRFS_COMPRESSION_EXTENT_BLOCK_COUNT = 32
  DRIVE_COUNT = $drive_count

  def fragmentation_cost(size, seek_time)
    return 1 if size == 0
    # How much time needed to read data
    # initial seek, sequential read time, seek cost
    ideal = seek_delay + (size.to_f / transfer_rate)
    (ideal + seek_time) / ideal
  end

  # Passed offsets are in 4096 blocks
  # We compute the time wasted either flying over unused data or
  # seeking looking for data on another track
  def seek_time(from, to)
    distance = (from - to).abs * 4096.to_f
    # Due to compression filefrag can return overlapping extents for
    # consecutive ones
    if (to < from) &&
        ((from - to) < BTRFS_COMPRESSION_EXTENT_BLOCK_COUNT)
      distance = 0
    end
    if distance < TRACK_SIZE
      # Assume we waste time flying over data from another extent in the
      # same track
      TRACK_READ_DELAY * (distance / TRACK_SIZE)
    else
      # Assume the drive has to seek with a cost proportional to the distance
      MIN_SEEK + (MAX_SEEK - MIN_SEEK) * distance /
                 (TRACK_COUNT * TRACK_SIZE * DRIVE_COUNT)
    end
  end

  def transfer_rate
    TRANSFER_RATE * DRIVE_COUNT
  end
  def seek_delay
    SEEK_DELAY
  end

end

class FilefragParser
  include Outputs
  include FragmentationCost

  def initialize
    reinit
  end

  # This handles the actual parsing, we use binary encoding for regexp to match
  # expected string encoding
  def add_line(line)
    @buffer << line
    case line
    when /^Filesystem type is:/n,
         /^ ext:     logical_offset:        physical_offset: length:   expected: flags:/n
      # Headers, ignored
    when /^File size of (.+) is (\d+) /n
      @filesize = Regexp.last_match(2).to_i
      @filename = Regexp.last_match(1)
    when /^(.+): \d+ extents? found$/n
      if @filename != Regexp.last_match(1)
        error("Couldn't understand this part:\n" +
              @buffer.join("\n") +
              "\n** #{@filename} ** !=\n** #{Regexp.last_match(1)} **")
      else
        @eof = true
      end
    when /^\s*\d+:\s*\d+\.\.\s*\d+:\s*(\d+)\.\.\s*(\d+):\s*(\d+):\s*(\d+):\s?(\S*)$/n
      # Example of such a line:
      #    1:      960..    1023:  193214976.. 193215039:     64:  193217920: last,eof
      @total_seek_time +=
        seek_time(Regexp.last_match(4).to_i, Regexp.last_match(1).to_i)
      @last_offset = Regexp.last_match(2).to_i
      flags = Regexp.last_match(5)
      length = Regexp.last_match(3).to_i
      if flags.split(",").include?("encoded")
        @compressed_blocks += length
      else
        @uncompressed_blocks += length
      end
    when /^\s*\d+:\s*\d+\.\.\s*\d+:\s*(\d+)\.\.\s*(\d+):\s*(\d+):\s*(\S*)$/n
      # Either first line or continuation of previous extent
      unless @total_seek_time == 0 ||
             Regexp.last_match(1).to_i == (@last_offset + 1)
        error("** Last line looks like a first line **\n" + buffer_dump)
      end
      @last_offset = Regexp.last_match(2).to_i
      flags = Regexp.last_match(4)
      length = Regexp.last_match(3).to_i
      if flags.split(",").include?("encoded")
        @compressed_blocks += length
      else
        @uncompressed_blocks += length
      end
    else
      error("** unknown line **\n" + buffer_dump)
    end
  rescue StandardError => ex
    error("** unprocessable line **\n" + buffer_dump)
    error ex.to_s
    error ex.backtrace.join("\n")
  end

  def buffer_dump
    if !$debug && @buffer.size > 12
      skipped = "  [... skipped #{@buffer.size - 10} lines ...]  "
      (@buffer[0..6] + [ skipped ] + @buffer[-3..-1]).join("\n")
    else
      @buffer.join("\n")
    end
  end

  # Prepare for a new file
  def reinit
    @filename = nil
    @total_seek_time = 0
    @last_offset = 0
    @filesize = 0
    @compressed_blocks = 0
    @uncompressed_blocks = 0
    @eof = false
    @buffer = []
  end

  # Test if a file is completely parsed
  def eof?
    @eof
  end

  def file_fragmentation(btrfs)
    frag = FileFragmentation.new(@filename, btrfs, majority_compressed)
    frag.fragmentation_cost =
      fragmentation_cost(@filesize, @total_seek_time)
    return frag
  end

  def current_frag_cost
    fragmentation_cost(@filesize, @total_seek_time)
  end

  def size
    @filesize
  end

  def majority_compressed
    @compressed_blocks > @uncompressed_blocks
  end
end

# Store fragmentation cost and relevant information for a file
class FileFragmentation
  attr_reader(:short_filename, :size, :fragmentation_cost)

  def initialize(filename, btrfs, majority_compressed)
    @short_filename = btrfs.short_filename(filename)
    @btrfs = btrfs
    @fragmentation_cost = 1
    @majority_compressed = majority_compressed
    @size = 0
  end

  def filename
    @btrfs.full_filename(@short_filename)
  end

  def fragmentation_cost=(cost)
    @fragmentation_cost = cost
  end

  def size=(size)
    @size = size
  end

  def defrag_time
    time = read_time +
           (write_time *
            @btrfs.average_cost(compress_type))
    time *= EXPECTED_COMPRESS_RATIO if compress_type == :compressed
    time
  end
  def majority_compressed?
    @majority_compressed
  end
  def compress_type
    @majority_compressed ? :compressed : :uncompressed
  end
  def human_size
    suffixes = [ "B", "kiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB" ]
    prefix = @size.to_f
    while (prefix > 1024) && (suffixes.size > 1)
      prefix = prefix / 1024
      suffixes.shift
    end
    "%.4g%s" % [ prefix, suffixes.shift ]
  end

  # Note: we don't use run_with_device_usage here because this is done
  # shortly after trying to defragment so we should always hit the cache
  def update_fragmentation
    unless File.file?(filename)
      @fragmentation_cost = 1
      return
    end

    IO.popen(["filefrag", "-v", filename], external_encoding: "BINARY") do |io|
      parser = FilefragParser.new
      while line = io.gets do
        parser.add_line(line.chomp!)
        if parser.eof?
          self.fragmentation_cost = parser.current_frag_cost
          @size = parser.size
          @majority_compressed = parser.majority_compressed
        end
      end
    end
  end

  def fs_commit_delay
    @btrfs.commit_delay
  end

  private

  def read_time
    ((@size / @btrfs.transfer_rate) * @fragmentation_cost) + @btrfs.seek_delay
  end
  def write_time
    (@size / @btrfs.transfer_rate) + @btrfs.seek_delay
  end

  class << self
    def batch_init(filelist, btrfs)
      frags = []
      until filelist.empty?
        files = filelist[0...FILEFRAG_ARGCOUNT_MAX]
        filelist = filelist[FILEFRAG_ARGCOUNT_MAX..-1] || []
        frags += batch_step(files, btrfs)
      end
      btrfs.track_compress_type(frags.map(&:compress_type)) if frags.any?
      frags
    end

    def batch_step(files, btrfs)
      sleep btrfs.delay_until_available_for_filefrag
      frags = []
      btrfs.run_with_device_usage do
        start = Time.now
        IO.popen([ "filefrag", "-v" ] + files,
                 external_encoding: "BINARY") do |io|
          parser = FilefragParser.new
          while line = io.gets do
            parser.add_line(line.chomp!)
            if parser.eof?
              frags << parser.file_fragmentation(btrfs)
              parser.reinit
            end
          end
        end
        btrfs.register_filefrag_speed(files.size, Time.now - start)
      end
      frags
    end
  end
end

class WriteEvents
  attr :first, :last
  def initialize
    @first = @last = Time.now
  end
  def write!
    @last = Time.now
  end
  # We suppose a file which is recently modified has higher chances of a
  # near-future modification, so we delay its check for defragmentation
  # commit_delay is used to avoid triggering fragmentation checks too early
  # by experience the fragmentation information is only updated after commits,
  # not after writes
  def ready_for_frag_check?(commit_delay)
    now = Time.now
    # We add a small delay to account for unexpected latencies
    after_write_delay = commit_delay * FRAGMENTATION_INFO_DELAY_FACTOR
    (last < (now - after_write_delay - fuzzy_delay)) ||
      (first < (now - MAX_WRITES_DELAY))
  end

  private

  # This avoids an avalanche of defragmentation checks, use most noisy
  # bits of first timestamp
  def fuzzy_delay
    first.usec % DEFRAG_CHECK_DISTRIBUTION_PERIOD
  end
end

# Maintain the status of all candidates for defragmentation for a given
# filesystem
class FilesState
  include Outputs
  include HashEntrySerializer

  TYPES = [ :uncompressed, :compressed ]
  attr_reader :last_queue_overflow_at

  # Track recent events using a compact bitarray indexed by hashes of objects
  # It isn't thread safe but in the event of a race condition
  # it can only create minor false recent positive/negative recent test results
  # This is considered acceptable and may be protected by caller if needed
  # Idea: we can modify the tick delay value dynamically to try to keep size
  # below a portion of the bitarray (minimizing the false positive probability)
  class FuzzyEventTracker
    # Must be 1, 2, 4 or 8 depending on the precision objective
    # higher value can avoid temporary high spikes of queued files
    BITS_PER_ENTRY = 8
    # Should be enough: we don't expect to defragment more than 1/s
    # there's an hardcoded limit of 24 in position_offset
    ENTRIES_INDEX_BITS = 16
    MAX_ENTRIES = 2 ** ENTRIES_INDEX_BITS
    ENTRIES_PER_BYTE = 8 / BITS_PER_ENTRY
    MAX_ENTRY_VALUE = (2 ** BITS_PER_ENTRY) - 1
    # How often a file is changing its hash data source to avoid colliding
    # with the same other files
    ROTATING_PERIOD = 7 * 24 * 3600 # one week
    # Split the objects in smaller groups that rotate independently
    # to avoid spikes on rotations
    ROTATE_GROUPS = 2 ** 16
    ROTATE_SEGMENT = ROTATING_PERIOD.to_f / ROTATE_GROUPS
    # Over which period approximately do we track the event rate
    EVENT_RATE_PERIOD = 30 * 60

    attr_reader :size

    def initialize(serialized_data = nil)
      @tick_interval =
        IGNORE_AFTER_DEFRAG_DELAY.to_f / ((2 ** BITS_PER_ENTRY) - 1)
      # Reset recent data if rules changed or invalid serialization format
      if !serialized_data || !serialized_data["ttl"] ||
         (serialized_data["ttl"] > IGNORE_AFTER_DEFRAG_DELAY) ||
         (serialized_data["bits_per_entry"] != BITS_PER_ENTRY) ||
         ((serialized_data["bitarray"].size * ENTRIES_PER_BYTE) != MAX_ENTRIES)
        dump = serialized_data && serialized_data.reject{|k,v| k == "bitarray"}
        info "Invalid serialized data: \n#{dump.inspect}"
        @bitarray =
          "\0".force_encoding(Encoding::ASCII_8BIT) *
          (MAX_ENTRIES / ENTRIES_PER_BYTE)
        @last_tick = Time.now
        @size = 0
        return
      end
      @bitarray = serialized_data["bitarray"]
      @last_tick = serialized_data["last_tick"]
      @size = serialized_data["size"]
      @bitarray.force_encoding(Encoding::ASCII_8BIT)
    end

    # Expects a string
    # set value to max in the entry (0 will indicate entry has expired)
    def event(object_id)
      advance_clock_when_needed
      position, offset = position_offset(object_id)

      byte = @bitarray.getbyte(position)
      nibbles = []
      ENTRIES_PER_BYTE.times do
        nibbles << (byte & MAX_ENTRY_VALUE)
        byte = (byte >> BITS_PER_ENTRY)
      end
      previous_value = nibbles[offset]
      nibbles[offset] = MAX_ENTRY_VALUE
      nibbles.reverse.each do |value|
        byte = (byte << BITS_PER_ENTRY)
        byte += value
      end
      @bitarray.setbyte(position, byte)
      @size += 1 if previous_value == 0
    end

    def recent?(object_id)
      advance_clock_when_needed
      position, offset = position_offset(object_id)
      byte = @bitarray.getbyte(position)
      offset.times { byte = (byte >> BITS_PER_ENTRY) }
      data = (byte & MAX_ENTRY_VALUE)
      data != 0
    end

    def serialization_data
      {
        "bitarray" => @bitarray,
        "last_tick" => @last_tick,
        "size" => @size,
        "ttl" => IGNORE_AFTER_DEFRAG_DELAY,
        "bits_per_entry" => BITS_PER_ENTRY
      }
    end

    private

    def advance_clock_when_needed
      tick! while (@last_tick + @tick_interval) < Time.now
    end

    # Rewrite bitarray, decrementing each value and updating size
    def tick!
      info "## FuzzyEventTracker size was: #{size}" if $debug
      @last_tick += @tick_interval
      return if @size == 0 # nothing to be done
      @size = 0
      @bitarray.size.times do |byte_idx|
        byte = @bitarray.getbyte(byte_idx)
        next if byte == 0 # should be common
        nibbles = []
        ENTRIES_PER_BYTE.times do
          entry = byte & MAX_ENTRY_VALUE
          entry = [ entry - 1, 0 ].max
          nibbles << entry
          byte = (byte >> BITS_PER_ENTRY)
        end
        nibbles.reverse.each do |value|
          @size += 1 if value > 0
          byte = (byte << BITS_PER_ENTRY)
          byte += value
        end
        @bitarray.setbyte(byte_idx, byte)
      end
      info "## FuzzyEventTracker size is: #{size}" if $debug
    end

    # The actual position slowly changes (once every ROTATING_PERIOD)
    def position_offset(object_id)
      object_digest = Digest::MD5.digest(object_id)
      verylong_int =
        object_digest.unpack('N*').
        each_with_index.map { |a, i| a * (2**32)**i }.inject(&:+)
      # This distributes object_ids so that they don't rotate simultaneously
      # which would create large spikes in new objects to defragment
      rotating_offset = verylong_int & (ROTATE_GROUPS - 1)
      rotation = ((Time.now.to_i + (rotating_offset * ROTATE_SEGMENT)) /
                  ROTATING_PERIOD).to_i
      # verylong_int is a 128 bit integer and we need to get 24 bits (3 bytes)
      digest_offset = rotation % 104
      # We get the position from 3 bytes (24 bits)
      event_idx = ((verylong_int >> digest_offset) & 16777215) % MAX_ENTRIES
      [ event_idx / ENTRIES_PER_BYTE, event_idx % ENTRIES_PER_BYTE ]
    end
  end

  def initialize(btrfs)
    @btrfs = btrfs
    # Queue of files to defragment, ordered by fragmentation cost
    @file_fragmentations = {
      compressed: [],
      uncompressed: [],
    }
    @last_queue_overflow_at = nil
    @fetch_accumulator = {
      compressed: 0.5,
      uncompressed: 0.5,
    }
    @fragmentation_info_mutex = Mutex.new
    @type_tracker = {
      compressed: 1.0,
      uncompressed: 1.0,
    }
    @tracker_mutex = Mutex.new
    load_recently_defragmented
    load_cost_history

    @written_files = {}
    @writes_mutex = Mutex.new
    @last_writes_consolidated_at = @next_status_at = Time.now
  end

  def any_interesting_file?
    @fragmentation_info_mutex.synchronize {
      TYPES.any? { |t| @file_fragmentations[t].any? }
    }
  end

  def queued
    @fragmentation_info_mutex.synchronize {
      @file_fragmentations.values.map(&:size).inject(&:+)
    }
  end

  def next_defrag_duration
    @fragmentation_info_mutex.synchronize {
      last_filefrag = @file_fragmentations[next_available_type].last
      last_filefrag ? last_filefrag.defrag_time : nil
    }
  end

  # Implement a weighted round-robin
  def pop_most_interesting
    @fragmentation_info_mutex.synchronize {
      # Choose type before updating the accumulators to avoid inadvertedly
      # switching to another type after calling next_defrag_duration
      current_type = next_available_type
      @fetch_accumulator[current_type] %= 1.0
      TYPES.each { |type| @fetch_accumulator[type] += type_share(type) }
      @file_fragmentations[current_type].pop
    }
  end

  def recently_defragmented?(shortname)
    @fragmentation_info_mutex.synchronize do
      @recently_defragmented.recent?(shortname)
    end
  end

  def defragmentation_rate(period: COST_HISTORY_TTL)
    cleanup_cost_history
    rate = 0
    now = Time.now.to_i
    start = now - period
    TYPES.each do |key|
      key_history = @cost_achievement_history[key]
      if period < COST_HISTORY_TTL
        key_history = key_history.select { |a| a[3] >= start }
      end
      # We might not cover the whole period if we have COST_HISTORY_SIZE
      rate += if key_history.size == COST_HISTORY_SIZE
                COST_HISTORY_SIZE.to_f / [ (now - key_history.first[3]), 1 ].max
              else
                key_history.size.to_f / period
              end
    end
    rate
  end

  # TODO: maybe only track recent defragmentations for WriteEvents-triggered
  # deframentations
  # (this would keep it small and should have the same behaviour)
  def defragmented!(shortname)
    @fragmentation_info_mutex.synchronize do
      @recently_defragmented.event(shortname)
      serialize_recently_defragmented if must_serialize_recent?
    end
    remove_tracking(shortname)
  end

  def remove_tracking(shortname)
    @writes_mutex.synchronize { @written_files.delete(shortname) }
  end

  def tracking_writes?(shortname)
    @writes_mutex.synchronize { @written_files.include?(shortname) }
  end

  # This adds or updates work to do based on fragmentations
  def update_files(file_fragmentations)
    return 0 unless file_fragmentations.any?

    updated_names = file_fragmentations.map(&:short_filename)
    # Remove files we won't consider anyway
    file_fragmentations.reject! { |frag| below_threshold_cost(frag) }
    @fragmentation_info_mutex.synchronize do
      # Remove duplicates
      # they will be replaced in queue later if still above threshold)
      duplicate_names = []
      TYPES.each do |type|
        @file_fragmentations[type].reject! do |frag|
          short_name = frag.short_filename
          if updated_names.include?(short_name)
            duplicate_names << short_name
            true
          end
        end
      end
      # Insert new versions and new files
      file_fragmentations.each do |f|
        @file_fragmentations[f.compress_type] << f
      end
      # Keep the order to easily fetch the worst fragemented ones
      sort_files
      # Remove old entries and fit in max queue length
      cleanup_files
      # Return number of queued items, ignoring duplicates
      (updated_names - duplicate_names).select do |n|
        # Note: uses uncompressed first as it seems the most common case
        TYPES.any? do |type|
          @file_fragmentations[type].any? { |f| f.short_filename == n }
        end
      end.size
    end
  end

  def file_written_to(filename)
    status
    shortname = @btrfs.short_filename(filename)
    return if recently_defragmented?(shortname)
    @writes_mutex.synchronize {
      if @written_files[shortname]
        @written_files[shortname].write!
      else
        @written_files[shortname] = WriteEvents.new
      end
    }
    if @last_writes_consolidated_at <
        (Time.now - TRACKED_WRITTEN_FILES_CONSOLIDATION_PERIOD)
      consolidate_writes
    end
  end

  def historize_cost_achievement(file_frag, initial_cost, final_cost, size)
    key = file_frag.majority_compressed? ? :compressed : :uncompressed
    history = @cost_achievement_history[key]
    history << [ initial_cost, final_cost, size, Time.now.to_i ]
    compute_thresholds
    serialize_history
  end

  def average_cost(type)
    @average_costs[type]
  end

  def below_threshold_cost(frag)
    frag.fragmentation_cost <= threshold_cost(frag)
  end

  def type_track(types)
    @tracker_mutex.synchronize {
      types.each { |type|
        @type_tracker[type] += 1.0
      }
      total = @type_tracker.values.inject(&:+)
      memory = 10_000.0 # TODO: tune/config
      if total > memory
        @type_tracker.each_key { |key|
          @type_tracker[key] *= (memory / total)
        }
      end
    }
  end

  def type_share(type)
    @tracker_mutex.synchronize {
      total = @type_tracker.values.inject(&:+)
      # Return a default value if there isn't enough data, assume equal shares
      (total < 10) ? 0.5 : @type_tracker[type] / total
    }
  end

  def queue_fill_proportion
    @fragmentation_info_mutex.synchronize {
      total_queue_size.to_f / MAX_QUEUE_LENGTH
    }
  end

  def status
    now = Time.now
    return if @next_status_at > now
    last_compressed_cost =
      @fragmentation_info_mutex.synchronize {
      @file_fragmentations[:compressed][0] ?
        "%.2f" % @file_fragmentations[:compressed][0].fragmentation_cost :
        "none"
    }
    last_uncompressed_cost =
      @fragmentation_info_mutex.synchronize {
      @file_fragmentations[:uncompressed][0] ?
        "%.2f" % @file_fragmentations[:uncompressed][0].fragmentation_cost :
        "none"
    }
    cleanup_cost_history
    if @cost_achievement_history[:compressed].any?
      info(("# #{@btrfs.dirname} c: %.1f%%; " \
            "Queued (c/u): %d/%d " \
            "C: %.2f>%.2f,q:%s,t:%.2f " \
            "U: %.2f>%.2f,q:%s,t:%.2f " \
            "flw: %d; recent: %d, %s") %
           [ type_share(:compressed) * 100,
             queue_size(:compressed), queue_size(:uncompressed),
             @initial_costs[:compressed], @average_costs[:compressed],
             last_compressed_cost, @cost_thresholds[:compressed],
             @initial_costs[:uncompressed], @average_costs[:uncompressed],
             last_uncompressed_cost, @cost_thresholds[:uncompressed],
             @written_files.size, @recently_defragmented.size,
             @btrfs.scan_status ])
    else
      info(("# #{@btrfs.dirname} (0cmpr); " \
            "Queued: %d %.2f>%.2f,q:%s,t:%.2f " \
            "flw: %d; recent: %d, %s") %
           [ queue_size(:uncompressed),
             @initial_costs[:uncompressed], @average_costs[:uncompressed],
             last_uncompressed_cost, @cost_thresholds[:uncompressed],
             @written_files.size, @recently_defragmented.size,
             @btrfs.scan_status ])
    end
    # We might skip a few on occasion
    @next_status_at += STATUS_PERIOD while now > @next_status_at
  end

  private
  # MUST be protected by @fragmentation_info_mutex
  def next_available_type
    next_type =
      @fetch_accumulator.keys.find { |type| @fetch_accumulator[type] >= 1.0 }
    # In case of Float rounding errors we might not have any available next_type
    case next_type
    when :compressed
      @file_fragmentations[:compressed].any? ? :compressed : :uncompressed
    else
      @file_fragmentations[:uncompressed].any? ? :uncompressed : :compressed
    end
  end

  def consolidate_writes
    batch = @writes_mutex.synchronize do
      candidates = []; to_check = []
      # Detect writen files whose fragmentation should be checked
      @written_files.each do |shortname, value|
        next unless value.ready_for_frag_check?(@btrfs.commit_delay)
        candidates << shortname
        fullname = @btrfs.full_filename(shortname)
        next unless File.file?(fullname)
        to_check << fullname
      end

      # Remove them from @written_files because update_files filters
      # according to its content
      candidates.each { |shortname| @written_files.delete(shortname) }

      # Cleanup written_files if it overflows, moving files to the
      # defragmentation queue
      if @written_files.size > MAX_TRACKED_WRITTEN_FILES
        to_remove = @written_files.size - MAX_TRACKED_WRITTEN_FILES
        @written_files.keys.sort_by { |short|
          @written_files[short].last
        }[0...to_remove].each { |short|
          fullname = @btrfs.full_filename(short)
          to_check << fullname if File.file?(fullname)
          @written_files.delete(short)
        }
        info "** %s writes tracking overflow: %d files queued for defrag" %
          [ @btrfs.dirname, to_remove ]
      end
      to_check
    end

    update_files(FileFragmentation.batch_init(batch, @btrfs))
    @last_writes_consolidated_at = Time.now
  end

  def thresholds_expired?
    @last_thresholds_at < (Time.now - COST_COMPUTE_DELAY)
  end

  # each achievement is [ initial_cost, final_cost, file_size ]
  # file_size is currently ignored (the target was jumping around on filesystems
  # with very diverse file sizes)
  def compute_thresholds
    return unless thresholds_expired?
    cleanup_cost_history
    TYPES.each do |key|
      key_history = @cost_achievement_history[key]
      size = key_history.size
      if size == 0
        @cost_thresholds[key] = MIN_FRAGMENTATION_THRESHOLD
        @average_costs[key] = @initial_costs[key] = 1.0
        next
      end
      # We want a weighted percentile, higher weight for more recent costs
      threshold_weight = ((size + 1) * size) / 2
      # If we used the file size:
      # @cost_achievement_history[key].each_with_index { |costs, index|
      #   threshold_weight = costs[2] * (index + 1)
      # }
      threshold_weight *= (COST_THRESHOLD_PERCENTILE.to_f / 100)
      # Order by final_cost, transform weight to be between 1 and size
      ordered_history =
        key_history.each_with_index.map { |costs, index| [ costs, index + 1 ] }
                                   .sort_by { |a| a[0][1] }
      total_weight = 0
      final_accu = 0
      initial_accu = 0
      cost_achievement = [ 1, 1 ] # default if no cost found
      # This will stop as soon as we reach the percentile
      while total_weight < threshold_weight
        cost_achievement, weight = ordered_history.shift
        initial_accu += cost_achievement[0] * weight
        final_accu += cost_achievement[1] * weight
        total_weight += weight
      end
      # Percentile reached, normalize it
      @cost_thresholds[key] = normalize_cost_threshold(cost_achievement[1])
      # Continue with the rest to compute other stats
      while ordered_history.any?
        cost_achievement, weight = ordered_history.shift
        initial_accu += cost_achievement[0] * weight
        final_accu += cost_achievement[1] * weight
        total_weight += weight
      end
      @average_costs[key] = final_accu / total_weight
      @initial_costs[key] = initial_accu / total_weight
    end
    @last_thresholds_at = Time.now
  end
  def normalize_cost_threshold(cost)
    # The threshold can be raised so high by a succession of difficult files
    # that it would stop any defragmentation from happening and block the
    # threshold itself
    # Adapting based on the defragmentation rate safeguards against this
    rate = defragmentation_rate(period: COST_THRESHOLD_TRUST_PERIOD)
    if rate < COST_THRESHOLD_TRUST_LEVEL
      cost =
        ((cost * rate) +
         (MIN_FRAGMENTATION_THRESHOLD * (COST_THRESHOLD_TRUST_LEVEL - rate))) /
        COST_THRESHOLD_TRUST_LEVEL
    end
    [ cost, MIN_FRAGMENTATION_THRESHOLD ].max
  end
  def cleanup_cost_history
    oldest = Time.now.to_i - COST_HISTORY_TTL
    TYPES.each do |key|
      key_history = @cost_achievement_history[key]
      key_history.shift while key_history.size > COST_HISTORY_SIZE
      key_history.shift while key_history.any? && key_history.first[3] < oldest
    end
  end
  def must_serialize_history?
    @last_history_serialized_at < (Time.now - HISTORY_SERIALIZE_DELAY)
  end
  def serialize_history
    return unless must_serialize_history?
    serialize_entry(@btrfs.dir, @cost_achievement_history, HISTORY_STORE)
    @last_history_serialized_at = Time.now
  end
  def load_cost_history
    default_value = { compressed: [], uncompressed: [] }
    @cost_achievement_history = unserialize_entry(@btrfs.dir, HISTORY_STORE,
                                                  "cost history", default_value)

    # Update previous versions without timestamps
    step = COST_HISTORY_TTL / COST_HISTORY_SIZE
    now = Time.now.to_i
    TYPES.each do |key|
      history_key = @cost_achievement_history[key]
      index = history_key.size + 1
      @cost_achievement_history[key].map! do |cost|
        index -= 1
        cost.size == 4 ? cost : cost << (now - step * index)
      end
    end
    cleanup_cost_history

    @last_history_serialized_at = Time.now
    # Force thresholds computation
    @last_thresholds_at = Time.at(0)
    @cost_thresholds = {}
    @average_costs = {}
    @initial_costs = {}
    compute_thresholds
  end
  def must_serialize_recent?
    @last_recent_serialized_at < (Time.now - RECENT_SERIALIZE_DELAY)
  end
  def load_recently_defragmented
    @recently_defragmented =
      FuzzyEventTracker.new(unserialize_entry(@btrfs.dir, RECENT_STORE,
                                              "recently defragmented"))
    @last_recent_serialized_at = Time.now
  end
  # Note: must be called protected by @fragmentation_info_mutex
  def serialize_recently_defragmented
    serialize_entry(@btrfs.dir, @recently_defragmented.serialization_data,
                    RECENT_STORE)
    @last_recent_serialized_at = Time.now
  end

  def sort_files
    TYPES.each { |t| @file_fragmentations[t].sort_by!(&:fragmentation_cost) }
  end
  # Must be called protected by @fragmentation_info_mutex and after sorting
  def cleanup_files
    if total_queue_size > MAX_QUEUE_LENGTH
      @last_queue_overflow_at = Time.now
      compressed_target_size = queue_reserve(:compressed)
      uncompressed_target_size = queue_reserve(:uncompressed)
      if queue_size(:compressed) < compressed_target_size
        # uncompressed has this size available
        uncompressed_target_size = MAX_QUEUE_LENGTH - queue_size(:compressed)
      end
      if queue_size(:uncompressed) < uncompressed_target_size
        # compressed has this size available
        compressed_target_size = MAX_QUEUE_LENGTH - queue_size(:uncompressed)
      end
      if queue_size(:compressed) > compressed_target_size
        start = queue_size(:compressed) - compressed_target_size
        @file_fragmentations[:compressed] =
          @file_fragmentations[:compressed][start..-1]
      end
      if queue_size(:uncompressed) > uncompressed_target_size
        start = queue_size(:uncompressed) - uncompressed_target_size
        @file_fragmentations[:uncompressed] =
          @file_fragmentations[:uncompressed][start..-1]
      end
    end
  end
  def queue_size(type)
    @file_fragmentations[type].size
  end
  def queue_reserve(type)
    [ (MAX_QUEUE_LENGTH * type_share(type)).to_i, 2 ].max
  end
  def total_queue_size
    TYPES.map{ |t| queue_size(t) }.inject(&:+)
  end
  def threshold_cost(frag)
    compute_thresholds
    if frag.majority_compressed?
      @cost_thresholds[:compressed]
    else
      @cost_thresholds[:uncompressed]
    end
  end
end

# Responsible for maintaining information about the filesystem itself
# and background threads
class BtrfsDev
  attr_reader :dir, :dirname, :commit_delay
  include FragmentationCost
  include Outputs
  include HashEntrySerializer
  include HumanFormat

  # create the internal structures, including references to other mountpoints
  def initialize(dir, dev_fs_map)
    @dir = dir
    @dir_slash = dir.end_with?("/") ? dir : "#{dir}/"
    @dirname = File.basename(dir)
    @checker = UsagePolicyChecker.new(self)
    @files_state = FilesState.new(self)
    # Note: @files_state MUST be created before @rate_controller
    @rate_controller = FilesStateRateController.new(dev: self)

    # Tracking of file defragmentation
    @files_in_defragmentation = {}
    @stat_mutex = Mutex.new

    # Init filefrag time tracking and rate with sensible values
    @average_file_time = 0
    detect_options(dev_fs_map)
  end

  def detect_options(dev_fs_map)
    @my_dev_id = File.stat(dir).dev
    update_subvol_dirs(dev_fs_map)
    load_exceptions
    changed = false
    compressed = mounted_with_compress?
    if compressed.nil?
      info "= #{dir}: probably umounted"
      return
    end
    if @compression_algo != compression_algorithm
      changed = true
      @compression_algo = compression_algorithm
      if @compression_algo
        info "= #{dir}: now using compression #{@compression_algo}"
      else
        info "= #{dir}: compression disabled"
      end
    end
    commit_delay = parse_commit_delay
    if @commit_delay != commit_delay
      changed = true
      @commit_delay = commit_delay
      info "= #{dir}: commit_delay is now #{@commit_delay}"
    end
    # Reset defrag_cmd if something changed (will be computed on first access)
    @defrag_cmd = nil if changed

    # Note: @autodefrag == nil or true means we weren't processing
    if autodefrag? && @autodefrag == false
      info "= #{dir}: autodefrag on, ignoring now"
      stop_processing
      @autodefrag = true
    elsif !autodefrag? && @autodefrag != false
      start_processing
      info "= #{dir}: autodefrag off, processing now"
      @autodefrag = false
    end
  end

  def start_processing
    @stat_thread = Thread.new { handle_stat_queue_progress }
    @slow_scan_thread = Thread.new do
      info("## Beginning files list updater thread for #{dir}")
      slow_files_state_update(first_pass: true)
      loop do
        defragment_extent_and_subvolume_trees if $defragment_trees
        slow_files_state_update
      end
    end
    @defrag_thread = Thread.new do
      loop do
        defrag!
        #min_delay = delay_between_defrags
        sleep delay_until_available_for_defrag#(min_delay)
      end
    end
  end

  def stop_processing
    [ @slow_scan_thread, @stat_thread, @defrag_thread ].each do |thread|
      Thread.kill(thread) if thread
    end
  end

  def has_dev?(dev_id)
    @dev_list.include?(dev_id)
  end

  # Manage queue of asynchronously defragmented files
  def stat_queue(file_frag)
    # Initially queued_at and last_change must be equal
    queued_at = Time.now
    @stat_mutex.synchronize {
      @files_in_defragmentation[file_frag] = {
        queued_at: queued_at,
        last_change: queued_at,
        last_cost: file_frag.fragmentation_cost,
        start_cost: file_frag.fragmentation_cost,
        size: file_frag.size
      }
    }
  end

  def handle_stat_queue_progress
    loop do
      check_start = Time.now
      to_remove = []
      @stat_mutex.synchronize do
        @files_in_defragmentation.each { |file_frag, value|
          last_change = value[:last_change]
          defrag_time = Time.now - value[:queued_at]
          # Cases where we can stop and register the costs
          if value[:last_cost] == 1.0 ||
             (defrag_time >
              (file_frag.fs_commit_delay + (2 * STAT_QUEUE_INTERVAL)))
            to_remove << file_frag
            @files_state.historize_cost_achievement(file_frag,
                                                    value[:start_cost],
                                                    value[:last_cost],
                                                    value[:size])
          elsif defrag_time > file_frag.fs_commit_delay
            file_frag.update_fragmentation
            # if it went up its because of concurrent activity, ignore
            if file_frag.fragmentation_cost < value[:last_cost]
              value[:last_cost] = file_frag.fragmentation_cost
              value[:last_change] = Time.now
            end
          end
        }
        to_remove.each { |frag| @files_in_defragmentation.delete(frag) }
      end
      wait = (check_start + STAT_QUEUE_INTERVAL) - Time.now
      sleep wait if wait > 0
    end
  end

  def claim_file_write(filename)
    local_name = position_on_fs(filename)
    return false unless local_name
    @files_state.file_written_to(local_name) unless skip_defrag?(local_name)
    true
  end

  def position_on_fs(filename)
    return filename if filename.start_with?(@dir_slash)

    subvol_fs = @fs_map.keys.find { |fs| filename.start_with?(fs) }
    return nil unless subvol_fs

    # This is a local file, triggered from another subdir, move in our domain
    filename.gsub(subvol_fs, @fs_map[subvol_fs])
  end

  def defrag!
    file_frag = get_next_file_to_defrag
    return unless file_frag

    # To avoid long runs without status if there are files to defragment
    @files_state.status
    shortname = file_frag.short_filename
    # We declare it defragmented ASAP to avoid a double queue
    @files_state.defragmented!(shortname)
    run_with_device_usage do
      if $verbose
        # Deal with file deletion race condition
        mtime = File.mtime(file_frag.filename) rescue nil
        msg = " - %s: %s %s,%s,%.2f,%s" %
              [ dir, shortname, (file_frag.majority_compressed? ? "C" : "U"),
                file_frag.human_size, file_frag.fragmentation_cost,
                human_delay_since(mtime) ]
        info(msg)
      end
      system(*defrag_cmd, file_frag.filename)
    end
    # Clear up any write detected concurrently
    @files_state.remove_tracking(shortname)
    stat_queue(file_frag)
  end

  # Experimental, the impact of this isn't depicted in the BTRFS documentation
  # when defragmenting directories without -r we defragment additional
  # structures: extent and subvolume trees. Can destroy performance on HDD
  def defragment_extent_and_subvolume_trees
    # Defragment root subvolume
    run_with_device_usage do
      info " - #{dir}: root subvolume extent and subvolume trees" if $verbose
      system(*defrag_cmd, dir)
      info " - #{dir}: root subvolume trees done" if $verbose
    end
    @rw_subvols.each do |subvol|
      run_with_device_usage do
        info " - #{dir}: #{subvol} extent and subvolume trees" if $verbose
        system(*defrag_cmd, subvol)
        info " - #{dir}: #{subvol} trees done" if $verbose
      end
    end
  end

  # recursively search for the next file to defrag
  def get_next_file_to_defrag
    return nil unless @files_state.any_interesting_file?

    file_frag = @files_state.pop_most_interesting
    shortname = file_frag.short_filename
    # Check that file still exists
    unless file_frag && File.file?(file_frag.filename)
      @files_state.remove_tracking(shortname)
      return get_next_file_to_defrag
    end
    file_frag.update_fragmentation
    # Skip files that are already below target and reset tracking
    # to avoid future work if there aren't any more writes
    if @files_state.below_threshold_cost(file_frag)
      @files_state.remove_tracking(shortname)
      return get_next_file_to_defrag
    end

    if @files_state.recently_defragmented?(shortname)
      @files_state.remove_tracking(shortname)
      return get_next_file_to_defrag
    end

    file_frag
  end

  # Only recompress if we are already compressing data
  # use -f to flush data (should allow more accurate disk usage stats)
  def defrag_cmd
    return @defrag_cmd if @defrag_cmd
    cmd =
      [ "btrfs", "filesystem", "defragment", "-t", $default_extent_size, "-f" ]
    cmd << "-c#{comp_algo_param_value}" if @compression_algo
    @defrag_cmd = cmd
  end

  def comp_algo_param_value
    @compression_algo.gsub(/:.*$/, '')
  end

  # We prefer to store short filenames to free memory
  def short_filename(filename)
    filename.gsub("#{dir}/", "")
  end
  def full_filename(short_filename)
    "#{dir}/#{short_filename}"
  end
  def file_id(short_filename)
    "#{@dirname}: #{short_filename}"
  end
  def average_cost(type)
    @files_state.average_cost(type)
  end
  def track_compress_type(compress_types)
    @files_state.type_track(compress_types)
  end

  def run_with_device_usage(&block)
    @checker.run_with_device_usage(&block)
  end

  def delay_until_available_for_filefrag
    @checker.delay_until_available
  end

  def scan_status
    "%d/%.3fs (%.3fs expected, IO %.0f%%) speed:%.2f %s" %
      [ @rate_controller.slow_batch_size, @rate_controller.slow_batch_period,
        average_batch_time, @checker.load * 100, global_speed_factor,
        @rate_controller.scan_speed_rate(considered: @considered) ]
  end

  def register_filefrag_speed(count, time)
    file_time = time / count
    weight = time / 120
    @average_file_time = if weight >= 0.5
                           (@average_file_time + file_time) / 2
                         else
                           @average_file_time * (1 - weight) +
                             file_time * weight
                         end
    @rate_controller.set_slow_batch_target
  end

  # accelerate/slowdown based on queue length and IO load
  def global_speed_factor
    queue_speed_factor / @checker.expected_slowdown
  end

  private

  # This class handles batch processing, it expects
  # an initial batch size, and a block to call when the batch is ready
  # the block must return the new batch size
  class Batch
    attr_reader :filelist

    def initialize(batch_size:, &block)
      @block = block
      reset(batch_size: batch_size)
    end

    def add_ignored
      @ignored += 1
      handle_full_batch
    end

    def add_path(path)
      @filelist << path
      @arg_length += (path.size + 1) # count space
      handle_full_batch
    end

    def ready?
      ((@filelist.size + @ignored) >= @batch_size) ||
        (@arg_length >= FILEFRAG_ARG_MAX)
    end

    private

    def reset(batch_size:)
      @filelist = []
      @arg_length = 0
      @ignored = 0
      @batch_size = batch_size
    end

    def handle_full_batch
      return unless ready?

      new_batch_size = @block.call
      reset(batch_size: new_batch_size)
    end
  end

  class FilesStateRateController
    attr_accessor :target_stop_time
    attr :slow_batch_size, :slow_batch_period
    include HashEntrySerializer
    include HumanFormat

    def initialize(dev:)
      @pass = :none
      @caught_up = false
      @dev = dev
      load_filecount
      # Need to define default values until init_new_scan (some other threads
      # ask us about current rate status)
      @target_stop_time = Time.now + SLOW_SCAN_PERIOD
      init_slow_batch_target
    end

    def init_new_scan
      # @pass logic probably overkill
      case @pass
      when :none
        @pass = :first
        info("= #{@dev.dirname}: skipping #{@processed} files " \
             "in #{SLOW_SCAN_CATCHUP_WAIT}s")
        # Avoids hammering disk just after boot (see "--slow-start" option)
        sleep SLOW_SCAN_CATCHUP_WAIT
      when :first
        @pass = :non_first
      end
      @scan_start = Time.now
      # If we have to skip some files, skip the corresponding time period
      if @filecount && @filecount > 0 && @processed > 0
        # compute an approximate time start from the work already done
        @scan_start -= (SLOW_SCAN_PERIOD * @processed.to_f / @filecount)
      end
      @target_stop_time = @scan_start + SLOW_SCAN_PERIOD
      @last_slow_scan_batch_start = Time.now
      init_slow_batch_target
    end

    def wait_next_slow_scan_pass(considered:)
      update_filecount(processed: considered)
      previous_batch_time = Time.now - @last_slow_scan_batch_start
      sleep [ @slow_batch_period - previous_batch_time, 0 ].max
      @last_slow_scan_batch_start = Time.now
    end

    def set_slow_batch_target
      @slow_batch_size, @slow_batch_period = slow_batch_target
    end

    def slow_batch_target
      # If there isn't enough time or data, speed up
      time_left = scan_time_left # cache it to avoid future negative values
      return catching_up_batch_target(time_left: time_left) if time_left <= 0

      # Maintain cruising speed if expected files are found and have time left
      left = slow_scan_expected_left
      return pass_batch_with_speed_factor if left <= 0

      # If we don't know the filesystem yet make a fast first pass
      # (rework for large and busy filesystems ?)
      unless @filecount
        return [ MAX_FILES_BATCH_SIZE, MIN_DELAY_BETWEEN_FILEFRAGS ]
      end

      ## Adaptive speed during normal scan
      batch_target_for(files_left: left, time_left: time_left)
    end

    def catching_up?(considered:)
      return false if @caught_up
      return true if considered < @processed

      info "= #{@dev.dirname}: caught up #{@processed} files"
      @last_slow_scan_batch_start = Time.now
      @caught_up = true
      false
    end

    def update_filecount(processed:, total: nil)
      now = Time.now
      @processed = processed
      # Having a valid total is important (avoids a fast full scan on start)
      # and happens infrequently
      do_update = if (total && total != @filecount)
                    true
                  else
                    !@last_filecount_updated_at ||
                      (@last_filecount_updated_at <
                       (now - FILECOUNT_SERIALIZE_DELAY))
                  end
      return unless do_update

      @filecount = total || @filecount
      entry = { processed: @processed, total: @filecount }
      serialize_entry(@dev.dir, entry, FILE_COUNT_STORE)
      @last_filecount_updated_at = now
      info "# #{@dev.dirname}, #{entry.inspect}" if total && $debug
    end

    def scan_speed_rate(considered:)
      if @filecount.nil? || @filecount == 0
        "unknow"
      elsif scan_time == 0
        "init"
      else
        "%.1f%%" % (100 * (considered.to_f / @filecount) /
                    (scan_time.to_f / SLOW_SCAN_PERIOD)).to_f
      end
    end

    def wait_slow_scan_restart
      # If @filecount.nil? we were counting files, restart fast
      sleep (if @filecount && scan_time_left > 0
             [ [ scan_time_left, MIN_DELAY_BETWEEN_FILEFRAGS ].max,
               MAX_DELAY_BETWEEN_FILEFRAGS ].min
             else
               MIN_DELAY_BETWEEN_FILEFRAGS
             end)
    end

    def scan_time
      return 0 unless @scan_start

      Time.now - @scan_start
    end

    private

    def scan_time_left
      @target_stop_time - Time.now
    end

    def load_filecount
      default_value = { processed: 0, total: nil }
      entry = unserialize_entry(@dev.dir, FILE_COUNT_STORE, "filecount",
                                default_value)
      @processed = entry[:processed]
      @filecount = entry[:total]
    end

    # Get filecount or a default value
    def expected_filecount
      [ @filecount ||
        SLOW_SCAN_PERIOD * MAX_FILES_BATCH_SIZE / MIN_DELAY_BETWEEN_FILEFRAGS,
        1 ].max
    end

    def slow_scan_expected_left
      expected_filecount - @processed
    end

    def init_slow_batch_target
      @speed_increases = 0
      params = { files_left: expected_filecount,
                 time_left: SLOW_SCAN_PERIOD }
      @pass_target_speed =
        filefrag_speed_target(params.merge(ignore_speed_factor: true))
      @slow_batch_size, @slow_batch_period = batch_target_for(params)
    end

    def pass_batch_with_speed_factor
      speed_to_batch(speed: @pass_target_speed * @dev.global_speed_factor)
    end

    def catching_up_batch_target(time_left:)
      speedup = false
      while @speed_increases < (-time_left / filefrag_speed_increase_period)
        @speed_increases += 1
        # We increase the speed by SLOW_SCAN_SPEED_INCREASE_STEP each time
        speedup = true
      end
      adjusted_speed = SLOW_SCAN_SPEED_INCREASE_STEP ** @speed_increases
      if speedup
        info("= #{@dev.dirname}: speedup scan to catch up (x%.2f) for next %s" %
             [ adjusted_speed, human_duration(filefrag_speed_increase_period) ])
      end
      # Base the speed on the pass target speed computed at the pass beginning
      speed = adjusted_speed * @pass_target_speed * @dev.global_speed_factor
      speed_to_batch(speed: speed)
    end

    # Only makes sense for and supports time_left > 0 and files_left >= 0
    def batch_target_for(files_left:, time_left:)
      filefrag_rate = filefrag_speed_target(files_left: files_left,
                                            time_left: time_left)
      # Limit rate to avoid large speed ups at end of batch to compensate
      # for earlier slowdowns (the speed will go up later with @speed_increases)
      filefrag_rate = [ filefrag_rate,
                        @pass_target_speed * SLOW_SCAN_MAX_SPEED_FACTOR ].min
      speed_to_batch(speed: filefrag_rate)
    end

    def filefrag_speed_target(files_left:, time_left:,
                              ignore_speed_factor: false)
      base = files_left / time_left.to_f
      return base if ignore_speed_factor

      base * @dev.global_speed_factor
    end

    def speed_to_batch(speed:)
      batch_size =
        [ [ (MIN_DELAY_BETWEEN_FILEFRAGS * speed).ceil,
            MIN_FILES_BATCH_SIZE ].max, MAX_FILES_BATCH_SIZE ].min
      # to_f avoids ZeroDivisionError
      batch_period =
        [ [ batch_size / speed.to_f,
            MIN_DELAY_BETWEEN_FILEFRAGS ].max, MAX_DELAY_BETWEEN_FILEFRAGS ].min
      [ batch_size, batch_period ]
    end

    # We want to reach max filefrag speed in SLOW_BATCH_PERIOD / 2 if we spend
    # more than SLOW_BATCH_PERIOD to finish the filesystem slow scan
    def filefrag_speed_increase_period
      # This can't change, cache it
      return @filefrag_speed_increase_period if @filefrag_speed_increase_period
      max_speed_ratio =
        (MAX_DELAY_BETWEEN_FILEFRAGS / MIN_DELAY_BETWEEN_FILEFRAGS) *
        (MAX_FILES_BATCH_SIZE / MIN_FILES_BATCH_SIZE)
      logstep = Math.log(SLOW_SCAN_SPEED_INCREASE_STEP)
      logratio = Math.log(max_speed_ratio)
      steps_needed = logratio / logstep
      @filefrag_speed_increase_period = (SLOW_SCAN_PERIOD / 2) / steps_needed
    end

  end

  # Slowly update files, targeting a SLOW_SCAN_PERIOD period for all updates
  def slow_files_state_update(first_pass: false)
    @rate_controller.init_new_scan
    @considered = already_processed = recent = @queued = 0
    @slow_status_at = @last_slow_scan_batch_start = Time.now
    @batch = Batch.new(batch_size: @rate_controller.slow_batch_size) do
      queue_slow_scan_batch
      @rate_controller.wait_next_slow_scan_pass(considered: @considered)
      @rate_controller.slow_batch_size
    end
    begin
      Find.find(dir) do |path|
        slow_status(already_processed, recent)
        (Find.prune; next) if prune?(path)

        # ignore files with unparsable names
        short_name = short_filename(path) rescue ""
        next if short_name == ""
        # Only process file entries (File.file? is true for symlinks)
        next unless File.exists?(path)
        next if !File.file?(path) || File.symlink?(path)
        @considered += 1

        # Don't process during a resume
        next if @rate_controller.catching_up?(considered: @considered)

        # Ignore recently processed files
        if @files_state.recently_defragmented?(short_name)
          already_processed += 1
          @batch.add_ignored
          next
        end
        # Ignore tracked files
        if @files_state.tracking_writes?(short_name)
          recent += 1; @batch.add_ignored
          next
        end
        stat = File.stat(path) rescue nil
        # stat failed: file isn't processable (maybe deleted concurrently)
        next unless stat
        # Files small enough to fit a node can't be fragmented
        # We don't count it as if nothing changes it won't become a target
        (@considered -= 1; next) if stat.size <= 4096

        @batch.add_path(path)
      end
    rescue => ex
      error("Couldn't process #{dir}: " \
            "#{ex}\n#{ex.backtrace.join("\n")}")
      # Don't wait for a SLOW_SCAN_PERIOD but don't create load either
      @rate_controller.target_stop_time = Time.now + MAX_DELAY_BETWEEN_FILEFRAGS
    end
    # Process remaining files to update
    queue_slow_scan_batch
    # Store the amount of files found
    @rate_controller.update_filecount(processed: 0, total: @considered)
    files_state_update_report(already_processed, recent)
    @rate_controller.wait_slow_scan_restart
  end

  def prune?(entry)
    (File.directory?(entry) && (entry != dir) &&
     Pathname.new(entry).mountpoint? && !rw_subvol?(entry)) ||
      blacklisted?(entry)
  rescue
    # Pathname#mountpoint can't process some entries
    false
  end

  def skip_defrag?(filename)
    (@autodefrag != false) || filename.end_with?(" (deleted)") ||
      blacklisted?(filename)
  end

  def mounted_with_compress?
    options = mount_options
    return nil unless options
    options.split(',').all? do |option|
      !(option.start_with?("compress=") ||
        option.start_with?("compress-force="))
    end
  end

  def autodefrag?
    options = mount_options
    # Don't want to try to activate if we didn't get options
    return true unless options

    options.split(',').detect { |option| option == "autodefrag" }
  end

  def compression_algorithm
    options = mount_options
    return nil unless options
    compress_option = options.split(',').detect do |option|
      option.start_with?("compress=") ||
        option.start_with?("compress-force=")
    end
    return nil unless compress_option
    compress_option.split('=')[1]
  end

  def parse_commit_delay
    delay = nil
    options = mount_options
    return unless options
    options.split(',').each { |option|
      if option.match(/^commit=(\d+)/)
        delay = Regexp.last_match[1].to_i
        break
      end
    }
    return delay || DEFAULT_COMMIT_DELAY
  end

  # Reading /proc/mounts can sometime be long: avoid doing it too much
  def mount_line
    return @mount_line if mount_line_recent?
    @mount_line = File.read("/proc/mounts").lines.reverse.find do |line|
      line.match(/\S+\s#{dir}\s/)
    end
    @mount_line_fetched_at = Time.now
    @mount_line
  end

  def mount_line_recent?
    @mount_line_fetched_at &&
      @mount_line_fetched_at > (Time.now - (FS_DETECT_PERIOD / 2))
  end

  def mount_options
    mount_line && mount_line.match(/\S+\s\S+\sbtrfs\s(\S+)/)[1]
  end

  def queue_slow_scan_batch
    # Note: this tracks filefrag speed and adjust batch size/period
    frags = FileFragmentation.batch_init(@batch.filelist, self)
    @queued += @files_state.update_files(frags)
  end

  # Adjust filefrag call speed based on amount of queued files
  # We slow the scan above QUEUE_PROPORTION_EQUILIBRIUM and speed up below
  def queue_speed_factor
    queue_proportion = @files_state.queue_fill_proportion
    factor = if queue_proportion > QUEUE_PROPORTION_EQUILIBRIUM
               # linear scale between SLOW_SCAN_MIN_SPEED_FACTOR and 1
               ((queue_proportion - QUEUE_PROPORTION_EQUILIBRIUM) /
                (1 - QUEUE_PROPORTION_EQUILIBRIUM) *
                (SLOW_SCAN_MIN_SPEED_FACTOR - 1)) + 1
             else
               # linear scale between 1 and SLOW_SCAN_MAX_SPEED_FACTOR
               SLOW_SCAN_MAX_SPEED_FACTOR +
                 (queue_proportion / QUEUE_PROPORTION_EQUILIBRIUM *
                  (1 - SLOW_SCAN_MAX_SPEED_FACTOR))
             end
    factor / LoadCheck.instance.slowdown_ratio
  end

  def average_batch_time
    @average_file_time * @rate_controller.slow_batch_size
  end

  def slow_status(already_processed, recent)
    now = Time.now
    return if @slow_status_at > now
    # This handles large slowdowns and suspends without spamming the log
    @slow_status_at += SLOW_STATUS_PERIOD until @slow_status_at > now
    defrag_rate =
      @files_state.defragmentation_rate(period: COST_THRESHOLD_TRUST_PERIOD)
    msg = ("$ %s %d/%ds: %d queued / %d found, " \
           "%d recent defrag (fuzzy), %d tracked, %.1f defrag/h") %
          [ @dirname, @rate_controller.scan_time.to_i, SLOW_SCAN_PERIOD,
            @queued, @considered, already_processed, recent,
            3600 * defrag_rate ]
    if @files_state.last_queue_overflow_at &&
       (@files_state.last_queue_overflow_at > (now - SLOW_SCAN_PERIOD))
      msg +=
        " ovf: %ds ago" % (now - @files_state.last_queue_overflow_at).to_i
    end
    info msg
    # We call it there too because if there isn't writes it isn't called
    # so this ensures regular updates
    @files_state.status
  end

  def files_state_update_report(already_processed, recent)
    # Force slow status display
    @slow_status_at = Time.now
    info "$# %s full scan report" % @dirname
    slow_status(already_processed, recent)
  end

  def delay_until_available_for_defrag
    next_defrag_duration = @files_state.next_defrag_duration
    if next_defrag_duration
      @checker.delay_until_available(expected_time: next_defrag_duration,
                                     use_limit_factor:
                                       low_queue_defrag_speed_factor)
    else
      # avoid busy loop, this is the maximum wait delay_until_available
      # could return
      sleep DEVICE_LOAD_WINDOW
    end
  end

  # When the queue is low we slow down defrags
  def low_queue_defrag_speed_factor
    # At QUEUE_PROPORTION_EQUILIBRIUM we reach the max speed for defrags
    # But don't go below MIN_QUEUE_DEFRAG_SPEED_FACTOR
    [ [ @files_state.queue_fill_proportion / QUEUE_PROPORTION_EQUILIBRIUM,
        1 ].min, MIN_QUEUE_DEFRAG_SPEED_FACTOR ].max
  end

  def load_exceptions
    no_defrag_list = []
    exceptions_file = "#{dir}/#{DEFRAG_BLACKLIST_FILE}"
    if File.readable?(exceptions_file)
      no_defrag_list =
        File.read(exceptions_file).split("\n").map { |path| "#{dir}/#{path}" }
    end
    if no_defrag_list.any? && (@no_defrag_list != no_defrag_list)
      info "= #{dir} blacklist: #{no_defrag_list.inspect}"
    end
    @no_defrag_list = no_defrag_list
  end

  def blacklisted?(filename)
    @no_defrag_list.any? { |blacklist| filename.start_with?(blacklist) }
  end

  def rw_subvol?(dir)
    @rw_subvols.include?(dir)
  end

  def update_subvol_dirs(dev_fs_map)
    subvol_dirs_list = BtrfsDev.list_rw_subvolumes(dir)
    fs_map = {}
    dev_list = Set.new
    rw_subvols = Set.new
    # Note dev_fs_map may not have '/' terminated paths but we must use them
    # for fast and accurate subpath detection/substitution in "position_on_fs"
    subvol_dirs_list.each do |subvol|
      full_path = if subvol.end_with?("/")
                    "#{dir}/#{subvol}"
                  else
                    "#{dir}/#{subvol}/"
                  end
      # We need the original name for prune? to work
      rw_subvols << "#{dir}/#{subvol}"
      dev_id = File.stat(full_path).dev
      other_fs = dev_fs_map[dev_id]
      other_fs.each do |fs|
        fs = "#{fs}/" unless fs.end_with?("/")
        fs_map[fs] = full_path
      end
      dev_list << dev_id
    end
    dev_list << File.stat(dir).dev
    if fs_map != @fs_map
      info "= #{dir}: changed filesystem maps"
      info fs_map.inspect
      @fs_map = fs_map
    end
    @dev_list = dev_list
    @rw_subvols = rw_subvols
  end

  class << self
    def list_subvolumes(dir)
      new_subdirs = []
      IO.popen([ "btrfs", "subvolume", "list", dir ]) do |io|
        while line = io.gets do
          line.chomp!
          if match = line.match(/^.* path (.*)$/)
            new_subdirs << match[1]
          else
            error "can't parse #{line}"
          end
        end
      end
      new_subdirs
    end

    def list_rw_subvolumes(dir)
      list_subvolumes(dir).select do |subdir|
        File.writable?("#{dir}/#{subdir}")
      end
    end
  end
end

class BtrfsDevs
  def initialize
    @btrfs_devs = []
    @lock = Mutex.new
    @new_fs = false
  end

  def update!
    # Enumerate BTRFS filesystems, avoid autodefrag ones
    dirs = File.open("/proc/mounts", "r") do |f|
      f.readlines.map { |line| line.split(' ') }
        .select { |ary| ary[2] == 'btrfs' }
    end.map { |ary| [ ary[1], ary[3] ] }
    dev_fs_map = Hash.new { |hash, key| hash[key] = Set.new }
    dirs.each { |dir, options| dev_fs_map[File.stat(dir).dev] << dir }
    @lock.synchronize do
      umounted =
        @btrfs_devs.select { |dev| !dirs.map(&:first).include?(dev.dir) }
      umounted.each do |dev|
        info "= #{dev.dir} not mounted, disabling"
        dev.stop_processing
      end
      @btrfs_devs.reject! { |dev| umounted.include?(dev) }

      # Detect remount -o compress=... events and (no)autodefrag
      @btrfs_devs.each { |dev| dev.detect_options(dev_fs_map) }
      dirs.map(&:first).each do |dir|
        next unless top_volume?(dir)
        next if known?(dir)
        next if @btrfs_devs.map(&:dir).include?(dir)
        @btrfs_devs << BtrfsDev.new(dir, dev_fs_map)
        @new_fs = true
      end
      # Longer devs first to avoid a top dir matching a file
      # in a device mounted below
      @btrfs_devs.sort_by! { |dev| -dev.dir.size }
    end
  end

  def handle_file_write(file)
    @lock.synchronize { @btrfs_devs.find { |dev| dev.claim_file_write(file) } }
  end

  # To call to detect a new fs being added to the watch list
  def new_fs?
    copy = false
    @lock.synchronize do
      copy = @new_fs
      @new_fs = false
    end
    copy
  end

  def top_volume?(dir)
    BtrfsDev.list_subvolumes(dir).all? do |subdir|
      Pathname.new("#{dir}/#{subdir}").mountpoint?
    end
  end

  def known?(dir)
    dev_id = File.stat(dir).dev
    @btrfs_devs.any? { |btrfs_dev| btrfs_dev.has_dev?(dev_id) }
  rescue
    true # if File.stat failed, this is a race condition with a concurrent umount
  end
end

include Outputs

def fatrace_file_writes(devs)
  # This is a hack to avoid early restarts:
  # new_fs? waits for devs.update! to finish which can be a bit long
  # as it calls the btrfs command multiple times to parse dev properties
  sleep 0.5; devs.new_fs?
  cmd = [ "fatrace", "-f", "W" ]
  extract_write_re = /^[^(]+\([0-9]+\): [ORWC]+ (.*)$/
  loop do
    failed = false
    info("= Starting global fatrace thread")
    begin
      last_popen_at = Time.now
      IO.popen(cmd) do |io|
        begin
          while line = io.gets do
            # Skip btrfs commands (defrag mostly)
            next if line.start_with?("btrfs(")
            if match = line.match(extract_write_re)
              file = match[1]
              devs.handle_file_write(file)
            else
              error "Can't extract file from '#{line}'"
            end
            # TODO: Maybe don't check on each pass
            break if Time.now > (last_popen_at + FATRACE_TTL)
            break if devs.new_fs?
          end
        rescue => ex
          msg = "#{ex}\n#{ex.backtrace.join("\n")}"
          error "Error in inner fatrace thread: #{msg}"
          sleep 1 # Limit CPU load in case of bug
        end
      end
    rescue => ex
      failed = true
      error "Error in outer fatrace thread: #{ex}"
    end
    next unless failed
    # Arbitrary sleep to avoid CPU load in case of fatrace repeated failure
    delay = 60
    info("= Fatrace thread waiting #{delay}s before restart")
    sleep delay
  end
end

info "**********************************************"
info "** Starting BTRFS defragmentation scheduler **"
info "**********************************************"
devs = BtrfsDevs.new
devs.update!
Thread.new { fatrace_file_writes(devs) }

loop { sleep FS_DETECT_PERIOD; devs.update! }
