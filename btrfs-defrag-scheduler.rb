#!/usr/bin/ruby

require 'getoptlong'
require 'set'
require 'digest'
require 'thread'
require 'find'
require 'pathname'
require 'yaml'
require 'fileutils'
require 'singleton'
require 'logger'
require 'etc'

$defaults = {
  extent_size: 33554432,
  threshold: 1.05,
  speed_multiplier: 1.0,
  slow_start: 600,
  drive_count: 1
}

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
    supported size suffixes are the same than the ones btrfs fi defrag supports
    except 'K' which is too small to make sense
    default: #{$defaults[:extent_size]}

--defrag-chunk-size <value> (-k)
    if passed, split the file in chunks of this size when defragmenting
    pausing between chunks according to device usage limits
    supported suffixes are the same than target-extent-size
    this value should be a multiple of the target-extent-size and
    processable (readable) in a relatively short time
    default: none (process files in a single chunk)

--threshold <value> (-w)
    minimum fragmentation cost needed to trigger defragmentation
    cost: slowdown ratio of estimated read speed in fragmented state vs speed
          when stored in one continuous zone
    default: #{$defaults[:threshold]}

--verbose (-v)
    display information about defragmentations

--debug (-d)
    display internal processes information

--speed-multiplier <value> (-m)
    slows down (<1.0) or speeds up (>1.0) the defragmentation process
    the process try to avoid monopolizing the I/O bandwidth limiting
    time waiting for IO to ~40%, increasing to 2.5 doesn't put any limit
    to IO wait.
    - decrease this if you see IO latency/bandwidth problems
    - increase this if the scheduler can't keep up and displays overflows
    default: #{$defaults[:speed_multiplier]}

--slow-start <value> (-l)
    wait for <value> seconds before scanning the filesystems.
    The initial catch-up to find the point where the last scan aborted is IO
    intensive. This gives time to other processes to warm up the disk cache.
    files modified/created are still processed during this period
    default: #{$defaults[:slow_start]}

--drive-count <value> (-c)
    number of 7200rpm drives used by the filesystem
    this changes the cost of seeking when computing fragmentation costs
    more drives: less cost
    default: #{$defaults[:drive_count]}

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

--trees (-f)
     fully defragment the read/write subvolume by launching an extent
     and subvolume trees defragment after each full scan
     WARNING: IO performance on HDD can suffer greatly on large filesystems
     default: disabled
EOMSG
  exit
end

opts =
  GetoptLong.new([ '--help', '-h', '-?', GetoptLong::NO_ARGUMENT ],
                 [ '--verbose', '-v', GetoptLong::NO_ARGUMENT ],
                 [ '--debug', '-d', GetoptLong::NO_ARGUMENT ],
                 [ '--ignore-load', '-i', GetoptLong::NO_ARGUMENT ],
                 [ '--full-scan-time', '-s', GetoptLong::REQUIRED_ARGUMENT ],
                 [ '--threshold', '-w', GetoptLong::REQUIRED_ARGUMENT ],
                 [ '--trees', '-f', GetoptLong::NO_ARGUMENT ],
                 [ '--target-extent-size', '-e',
                   GetoptLong::REQUIRED_ARGUMENT ],
                 [ '--defrag-chunk-size', '-k', GetoptLong::REQUIRED_ARGUMENT ],
                 [ '--speed-multiplier', '-m', GetoptLong::REQUIRED_ARGUMENT ],
                 [ '--slow-start', '-l', GetoptLong::REQUIRED_ARGUMENT ],
                 [ '--drive-count', '-c', GetoptLong::REQUIRED_ARGUMENT ],
                 [ '--target-load', '-t', GetoptLong::REQUIRED_ARGUMENT ],
                 [ '--no-timestamp', '-n', GetoptLong::NO_ARGUMENT ])

# Latest recommendation from BTRFS developpers as of 2016
$defragment_trees = false
$verbose = false
$debug = false
$ignore_load = false
$target_load = nil
$log_timestamps = true
$drive_count = $defaults[:drive_count]
$extent_size = $defaults[:extent_size]
$chunk_size = nil
speed_multiplier = $defaults[:speed_multiplier]
fragmentation_threshold = $defaults[:threshold]
slow_start = $defaults[:slow_start]
scan_time = nil

def convert_size(size)
  size = size.upcase
  if matchdata = size.match(/^(\d+)([MGTPE])$/)
    return case matchdata[2]
           when 'M'
             matchdata[1].to_i * 2**20
           when 'G'
             matchdata[1].to_i * 2**30
           when 'T'
             matchdata[1].to_i * 2**40
           when 'P'
             matchdata[1].to_i * 2**50
           when 'E'
             matchdata[1].to_i * 2**60
           end
  else
    raise "Invalid size #{size}"
  end
end

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
  when '--threshold'
    threshold = arg.to_f
    next if threshold < 1
    fragmentation_threshold = threshold
  when '--trees'
    $defragment_trees = true
  when '--target-extent-size'
    $extent_size = convert_size(arg)
  when '--defrag-chunk-size'
    $chunk_size = convert_size(arg)
  when '--speed-multiplier'
    multiplier = arg.to_f
    next if multiplier <= 0
    speed_multiplier = multiplier
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
  when '--no-timestamp'
    $log_timestamps = false
  end
end
if $chunk_size && ($chunk_size % $extent_size) != 0
  STDERR.puts "## WARNING: defrag-chunk-size not multiple of target-extent-size"
  STDERR.puts "## chunk: #{$chunk_size}, extent: #{$extent_size}"
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
MIN_FRAGMENTATION_THRESHOLD = fragmentation_threshold
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
HISTORY_SERIALIZE_DELAY = $debug ? 10 : 3600
RECENT_SERIALIZE_DELAY = $debug ? 10 : 1800
FILECOUNT_SERIALIZE_DELAY = $debug ? 10 : 600

# How many files do we queue for defragmentation
MAX_QUEUE_LENGTH = 2000
# Slow down slow_scan when the queue of files to defragment is above this level
QUEUE_PROPORTION_EQUILIBRIUM = 0.05
# How much device time the program is allowed to use (including filefrag calls)
# time window => max_device_use_ratio
# the allowed use_ratio is reduced when:
# - system load exceeds target (to avoid avalanche effects)
# - the defrag queue is near empty (to avoid fast successions of defrag when
#   possible)
DEVICE_USE_LIMITS = {
  0.3  => 0.6 * speed_multiplier,
  3.0  => 0.5 * speed_multiplier,
  30.0 => 0.4 * speed_multiplier
}
DEVICE_LOAD_WINDOW = DEVICE_USE_LIMITS.keys.max
EXPECTED_COMPRESS_RATIO = 0.5

# How many files do we track for writes, we pass these to the defragmentation
# queue when write activity stops (this amount limits memory and CPU usage)
MAX_TRACKED_WRITTEN_FILES = 10_000
# Avoid too frequent selection of expired write events for defragmentation
# should be < STOPPED_WRITING_DELAY (see consolidate_writes)
MIN_WRITTEN_FILES_LOOKUP_PERIOD = 5
# No writes for that many seconds is interpreted as write activity stopped
STOPPED_WRITING_DELAY = 30
# Some files might be written to constantly, don't delay passing them to
# filefrag more than that
MAX_WRITES_DELAY = 8 * 3600
# How often do we check for defragmentation progress if there are none
DEFAULT_PERF_QUEUE_INTERVAL = 20

# Full refresh of fragmentation information on files happens in
# (pass number of hours on commandline if the default is not optimal for you)
SLOW_SCAN_PERIOD = (scan_time || 4 * 7 * 24) * 3600 # 1 month
SLOW_SCAN_CATCHUP_WAIT = slow_start
# If the scan didn't finish after the initial period by how much do we speed up
# this will be repeated until reaching max speed SLOW_SCAN_PERIOD / 2 after
# the initial period
SLOW_SCAN_SPEED_INCREASE_STEP = 1.1
# If a scan stops early, delay the next one at most this
MAX_DELAY_BETWEEN_SLOW_SCANS = 120

# These are used to compensate for deviation of the slow scan progress
# If we lag, set a higher target speed up to this factor
SLOW_SCAN_MAX_SPEED_FACTOR = 4
# Speedup when the scheduler lags behind, rate linearly increase reaching 2x
# when the delay reaches this proportion of SLOW_SCAN_PERIOD
# The actual speedup capped by the value of SLOW_SCAN_MAX_SPEED_FACTOR
SLOW_SCAN_MAX_SPEED_AT = 0.01
# During high load (detected by defragmentation process not being able to keep
# up) slow down up to this factor, high CPU load can push this further down
SLOW_SCAN_MIN_SPEED_FACTOR = 0.02

# Batch size constraints for full refresh thread
# don't make it so large that at cruising speed it could overflow the queue
# with only one batch
MAX_FILES_BATCH_SIZE = MAX_QUEUE_LENGTH / 4
# Constraints on the delay between batches
MIN_DELAY_BETWEEN_FILEFRAGS = 0.1
MAX_DELAY_BETWEEN_FILEFRAGS = 120.0
# Target delay. Note that batching filefrags noticeably lower CPU usage,
# this is high enough to get low CPU usage on FS with multi million files
DEFAULT_DELAY_BETWEEN_FILEFRAGS = 1.0
MAX_FILEFRAG_SPEED_WITH_DEFAULT_DELAY =
  MAX_FILES_BATCH_SIZE / DEFAULT_DELAY_BETWEEN_FILEFRAGS

# If the background thread consolidating writes can't be scheduled soon enough
# we trigger an emergency consolidation to avoid slowing down more as
# having a very large number of tracked written files slows the tracking itself
EMERGENCY_WRITE_CONSOLIDATION_THRESHOLD =
  MAX_TRACKED_WRITTEN_FILES + MAX_FILES_BATCH_SIZE

# We ignore files recently defragmented for 12 hours
IGNORE_AFTER_DEFRAG_DELAY = 12 * 3600

MIN_QUEUE_DEFRAG_SPEED_FACTOR = 0.2

# How often do we dump a status update
STATUS_PERIOD = 120 # every 2 minutes
SLOW_STATUS_PERIOD = 1800 # every 30 minutes
# How often do we check for new filesystems or umounted filesystems
FS_DETECT_PERIOD = $debug ? 10 : 300
# How often do we restart the fatrace thread ?
# there were bugs where fatrace would stop reporting modifications under
# some conditions (mounts or remounts, fatrace processes per mountpoint and
# old fatrace version), it might not apply anymore but this doesn't put any
# measurable load on the system and we are unlikely to miss files
FATRACE_TTL = 24 * 3600 # every day

# System dependent (reserve 100 for cmd and 4096 for one path entry)
FILEFRAG_ARG_MAX = 131072 - 100 - 4096

# Overflow handling
MAX_FILEFRAG_QUEUE_SIZE = 1000
# This queue processing should be fairly cheap (data should be in cache)
# allow it to grow more
MAX_PERF_QUEUE_SIZE = 500

# Where do we serialize our data
STORE_DIR        = "/root/.btrfs_defrag"
FILE_COUNT_STORE = "#{STORE_DIR}/filecounts.yml"
HISTORY_STORE    = "#{STORE_DIR}/costs.yml"
RECENT_STORE     = "#{STORE_DIR}/recent.yml"

# Default Btrfs commit delay when none is specified
# it's otherwise parsed from /proc/mounts
DEFAULT_COMMIT_DELAY = 30

# Per filesystem defrag blacklist
DEFRAG_BLACKLIST_FILE = ".no_defrag"

$logger = Logger.new(STDOUT)
if $log_timestamps
  $logger.formatter = proc do |severity, datetime, progname, msg|
    "%s: %s\n" % [ datetime.strftime("%Y%m%d %H%M%S"), msg ]
  end
else
  $logger.formatter = proc do |severity, datetime, progname, msg|
    "#{msg}\n"
  end
end
$logger.level = $debug ? Logger::DEBUG : Logger::INFO

module Outputs
  def error(msg)
    $logger.error msg
  end
  def info(msg)
    $logger.info msg
  end

  # These support strings or blocks
  def debug(msg = nil)
    return unless $logger.debug?

    msg ||= (block_given? ? yield : nil)
    $logger.debug msg
  end
  def verbose(msg = nil)
    return unless $verbose

    msg ||= (block_given? ? yield : nil)
    $logger.info msg
  end
end

module Delaying
  def delay_until(tstamp, min_sleep: 0)
    now = Time.now
    if tstamp <= now
      delay min_sleep
    else
      sleep [ tstamp - now, min_sleep ].max
    end
  end

  def delay(amount)
    return if amount <= 0

    sleep amount
  end
end

module AlgebraUtils
  def memory_avg(avg, memory, new_value)
    (avg * (memory - 1) + new_value) / memory
  end

  def scaling(value, source_range, destination_range)
    position = (value - source_range.begin) /
               (source_range.end - source_range.begin)
    destination_range.begin +
      (destination_range.end - destination_range.begin) * position
  end
end

Thread.abort_on_exception = true

# Shared code for classes needing permanent storage
# this doesn't write ASAP but wait at least MIN_COMMIT_DELAY for
# other writes to the same backing file and at most MAX_COMMIT_DELAY
# old entries are cleaned up if not accessed for more than MAX_AGE
class AsyncSerializer
  include Outputs
  include Singleton
  include Delaying

  MIN_COMMIT_DELAY = $debug ? 5 : 60      # 1 minute
  MAX_COMMIT_DELAY = $debug ? 30 : 300    # 5 minutes
  MAX_AGE = $debug ? 3600 : 7 * 24 * 3600 # 1 week
  # This makes sure we don't wait more than needed
  LOOP_MAX_DELAY = MIN_COMMIT_DELAY

  def initialize
    @store_tasks = {}
    @store_content = {}
    @async_writer_thread = Thread.new { store_loop }
    # Serialization requests are pushed and poped in this queue
    @serialization_queue = Queue.new
  end

  def serialize_entry(file, key, value)
    @serialization_queue.push [ file, key, value, Time.now.to_f ]
  end

  def unserialize_entry(file, key, op_id, default_value)
    file_load(file) unless @store_content[file]
    info("= #{op_id}, #{key}: %sloaded" %
         (!@store_content[file][key].nil? ? "" : "default "))
    @store_content[file][key] ||= { last_write: Time.now.to_f,
                                    data: default_value }
    entry = @store_content[file][key]
    # Convert old timestamps to match new internals
    if entry[:last_write].is_a?(Time)
      entry[:last_write] = entry[:last_write].to_f
    end
    @store_content[file][key][:data]
  end

  def stop_and_flush_all
    puts "= Storing state"
    Thread.kill @async_writer_thread
    # Killing the writer protects against concurrent writes
    process_serialization_queue
    @store_content.keys.each { |file| file_write(file) }
  end

  private

  def store_loop
    loop do
      process_serialization_queue
      files = @store_tasks.select do |file, tstamps|
        now = Time.now.to_f
        # If there's only one entry, no use waiting
        (@store_content[file].count == 1) ||
          # otherwise use delays to give a chance to others to join
          tstamps[:last_write] < (now - MIN_COMMIT_DELAY) ||
          tstamps[:first_write] < (now - MAX_COMMIT_DELAY)
      end.keys
      files.each { |file| @store_tasks.delete(file) }
      files.each { |file| file_write(file) }
      delay_until_next_write_check
    end
  end

  def process_serialization_queue
    until @serialization_queue.empty?
      file, key, value, now = @serialization_queue.pop
      @store_content[file][key] = { last_write: now, data: value }
      if @store_tasks[file]
        @store_tasks[file][:last_write] = now
      else
        @store_tasks[file] = { first_write: now, last_write: now }
      end
    end
  end

  def delay_until_next_write_check
    max_expire = Time.now.to_f + LOOP_MAX_DELAY
    next_expiration = @store_tasks.values.map do |v|
      [ v[:last_write] + MIN_COMMIT_DELAY,
        v[:first_write] + MAX_COMMIT_DELAY ].min
    end.min
    delay_until Time.at([ next_expiration, max_expire ].compact.min)
  end

  def cleanup_old_keys(file)
    now = Time.now.to_f
    # Cleanup old keys
    @store_content[file].delete_if do |key, value|
      to_delete = value[:last_write] < (now - MAX_AGE)
      info "= #{file}: #{key} removed (not accessed recently)" if to_delete
      to_delete
    end
  end

  def file_load(file)
    now = Time.now.to_f
    if File.file?(file)
      File.open(file, 'r:ascii-8bit') do |f|
        yaml = f.read
        @store_content[file] = if yaml.empty?
                                 {}
                               else
                                 begin
                                   YAML.load(yaml)
                                 rescue => ex
                                   info "#{file} load error: #{ex.message}"
                                   {}
                                 end
                               end
        info "= #{file} loaded"
      end
    else
      @store_content[file] = {}
    end
    # Migrate keys to new format
    hash = @store_content[file]
    hash.each do |key, value|
      unless value[:last_write]
        info "= #{file} migrating #{key} to timestamped value"
        hash[key] = { last_write: now, data: value }
      end
      if value[:last_write].is_a?(Time)
        value[:last_write] = value[:last_write].to_f
      end
    end
    cleanup_old_keys(file)
  end

  def file_write(file)
    cleanup_old_keys(file)
    to_store = dump(file)
    FileUtils.mkdir_p(File.dirname(file))
    File.open(file, File::RDWR|File::CREAT|File::BINARY, 0644) do |f|
      f.write(to_store)
      f.flush
      f.truncate(f.pos)
    end
  end

  def dump(file)
    YAML.dump(@store_content[file])
  end
end

module HashEntrySerializer
  def serialize_entry(file, key, value)
    AsyncSerializer.instance.serialize_entry(file, key, value)
  end

  def unserialize_entry(file, key, op_id, default_value = nil)
    AsyncSerializer.instance.unserialize_entry(file, key, op_id, default_value)
  end
end

module HumanFormat
  DELAY_MAPS = { 24 * 3600 => "d",
                 3600      => "h",
                 60        => "m",
                 1         => "s",
                 0.001     => "ms",
                 nil       => "now" }

  def human_size(size)
    suffixes = [ "B", "kiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB" ]
    prefix = size.to_f
    while (prefix > 1024) && (suffixes.size > 1)
      prefix = prefix / 1024
      suffixes.shift
    end
    "%.4g%s" % [ prefix, suffixes.shift ]
  end

  def human_delay_since(timestamp)
    return "unknown" unless timestamp

    delay = Time.now - timestamp
    human_duration(delay)
  end

  def human_duration(duration)
    DELAY_MAPS.each do |amount, suffix|
      return suffix unless amount
      return "%.1f%s" % [ duration / amount, suffix ] if duration >= amount
    end
  end
end

# Class that handles running simple asynchronous tasks
# each task is a block that returns the next tstamp it wants to run at
# doesn't support running without a task
class AsyncRunner
  include Outputs
  include Delaying
  include AlgebraUtils

  TIMING_MEMORY = 100

  def initialize(name)
    @name = name
    @tasks = {}
    @timings = {}
  end

  def run
    loop do
      now = Time.now
      @tasks.each do |block, params|
        next if params[:run_at] > now

        delay = now - params[:run_at]
        start = now
        @tasks[block][:run_at] = block.call
        now = Time.now
        record_timings(block, delayed: delay, runtime: now - start)
      end
      delay_until @tasks.values.map { |h| h[:run_at] }.min
    end
  end

  def add_task(name: "", time: Time.now, &block)
    @tasks[block] = { run_at: time, name: name }
    @timings[block] = { delayed: 0, runtime: 0 }
  end

  def dump_timings
    @timings.each do |block, timings|
      puts("++ %s, %s: delayed %.2f, runtime %.3f" %
           [ @name, @tasks[block][:name], @timings[block][:delayed],
             @timings[block][:runtime] ])
    end
  end

  private

  def record_timings(block, delayed:, runtime:)
    timings = @timings[block]
    timings[:delayed] = memory_avg(timings[:delayed], TIMING_MEMORY, delayed)
    timings[:runtime] = memory_avg(timings[:runtime], TIMING_MEMORY, runtime)
  end
end

# Common source for fresh load checking, avoid wasting time in all threads
# finding out if the load is fresh on each access and reloading it when needing
# this makes periodic updates in a dedicated thread
class LoadCheck
  include Singleton

  # 10 seconds seems small enough to detect changes with real-life impacts
  LOAD_VALIDITY_PERIOD = 10

  def initialize
    # Don't waste time if not needed
    return if $ignore_load

    # Make sure we have a valid value before being called
    update_load
    # Update load in the background
    Thread.new do
      loop do
        update_load
        sleep LOAD_VALIDITY_PERIOD
      end
    end
  end

  # We don't slow down until load_ratio > 1
  def slowdown_ratio
    return 1 if $ignore_load

    [ load_ratio, 1 ].max
  end

  private

  def load_ratio
    # Note: not protected by mutex because Ruby concurrent access semantics
    # don't allow this object to have transient invalid values
    @load_ratio
  end

  def update_load
    @load_ratio = cpu_load / target_load
  end

  # Returns the number of processors if a target wasn't specified
  # Warning Etc.nprocessors is restricted by CPU affinity
  def target_load
    $target_load || Etc.nprocessors
  end

  def cpu_load
    # Note: to_f ignores everything after the first float in the string
    File.read('/proc/loadavg').to_f
  end

end

# Limit disk available bandwidth usage
class UsagePolicyChecker
  include Outputs

  def initialize(btrfs)
    @btrfs = btrfs
    @device_uses = []
    # Used to avoid concurrent IO
    @mutex = Mutex.new
  end

  # Caller should use delay_until_available before using this
  def run_with_device_usage(&block)
    # Prevent concurrent accesses
    start, result = @mutex.synchronize { [ Time.now, block.call ] }
    add_usage(start, Time.now)
    result
  end

  def available_at(expected_time: 0, use_limit_factor: 1)
    DEVICE_USE_LIMITS.keys.map do |window|
      next_available_for(window, expected_time, use_limit_factor)
    end.max
  end

  # This is an approximation of the IO load generated by the scheduler
  def load
    activity = 0.0
    window_start = Time.now - DEVICE_LOAD_WINDOW
    @device_uses.each do |start, stop|
      next if stop <= window_start
      activity += stop - [ start, window_start ].max
    end
    activity / DEVICE_LOAD_WINDOW
  end

  def cleanup
    this_start = Time.now
    # Cleanup the device uses, remove everything ending before used window
    # work on a copy to avoid interfering with concurrent Enumerators
    new_device_uses = @device_uses.dup
    new_device_uses.shift while (first = new_device_uses.first) &&
                                first[1] < (this_start - DEVICE_LOAD_WINDOW)
    @device_uses = new_device_uses
  end

  private

  # Note: there is a rare race condition where this can be added to a previous
  # instance of @device_uses, made obsolete by cleanup, doesn't seem worth a
  # mutex overhead though
  def add_usage(start, stop)
    @device_uses << [ start, stop ]
  end

  def next_available_for(window, expected_time, use_limit_factor)
    now = Time.now
    use_factor = use_limit_factor / LoadCheck.instance.slowdown_ratio
    target = DEVICE_USE_LIMITS[window] * use_factor
    # When will it reach the target use_ratio ?
    delay = dichotomy(0..window, target, 0.01) do |wait|
      use_ratio(now, wait, window, expected_time)
    end
    now + delay
  end

  # Return expected use ratio when reaching start given known past activity
  # before start, and "future" activity (based on expected_time) after start
  def use_ratio(now, wait, window, expected_time)
    start = now + wait - window
    time_spent = 0
    @device_uses.each do |use_start, use_stop|
      next if use_stop < start
      time_spent += use_stop - [ use_start, start ].max
    end
    # We don't need to consider expected_time if there's no device_use
    return 0 if time_spent == 0
    # Anything else and we return a normal ratio between total expected time
    # spent in window and window
    expected_time_in_window = [ window - wait, expected_time ].min
    (time_spent + expected_time_in_window) / window
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
    debug { "## dichotomy: %d steps, %.2fs, result: %.2fs" %
            [ steps, (Time.now - start), max ] }
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
    # Special case of next one to handle separately
    when /^\s*\d+:\s*\d+\.\.\s*\d+:\s*0\.\.\s*0:\s*0:\s*last,unknown_loc,delalloc,eof$/n
    # Example of such a line:
    # 2:        2..      25:          0..         0:      0:             last,unknown_loc,delalloc,eof
      # Nothing to do, extent being delalloced, maybe on file truncate
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
    # These only match once per output, so test them later
    when /^Filesystem type is:/n,
         /^ ext:     logical_offset:        physical_offset: length:   expected: flags:/n
      # Headers, ignored
    when /^File size of (.+) is (\d+) /n
      @filesize = Regexp.last_match(2).to_i
      @filename = Regexp.last_match(1)
    when /^(.+): \d+ extents? found$/n
      if @filename != Regexp.last_match(1)
        error("** Couldn't understand this part:\n" +
              @buffer.join("\n") +
              "\n** #{@filename} ** !=\n** #{Regexp.last_match(1)} **")
      else
        @eof = true
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
    time = read_time + (write_time * @btrfs.average_cost(compress_type))
    time *= EXPECTED_COMPRESS_RATIO if compress_type == :compressed
    time
  end
  def majority_compressed?
    @majority_compressed
  end
  def compress_type
    @majority_compressed ? :compressed : :uncompressed
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
    include Delaying

    # Pass cached: true if files are expected to be in cache
    def create(filelist, btrfs, cached:)
      frags = []
      until filelist.empty?
        files = []
        length = 0
        while filelist.any? && length < FILEFRAG_ARG_MAX
          file = filelist.pop
          length += file.size + 1
          files << file
        end
        frags += batch_step(files, btrfs, cached: cached)
      end
      btrfs.track_compress_type(frags.map(&:compress_type)) if frags.any?
      frags
    end

    def batch_step(files, btrfs, cached:)
      delay_until btrfs.available_for_filefrag_at(files.size,
                                                  cached: cached)
      # Filter at the last possible moment to avoid too many open errors in log
      files.reject! { |file| !File.file?(file) }
      return [] unless files.any?

      # Delay start value setting (run_with_device_usage blocks on mutex)
      io_start = nil
      frags = []
      btrfs.run_with_device_usage do
        io_start = Time.now
        IO.popen([ "filefrag", "-v" ] + files,
                 external_encoding: "BINARY") do |io|
          parser = FilefragParser.new
          while line = io.gets do
            parser.add_line(line.chomp!)
            next unless parser.eof?

            frags << parser.file_fragmentation(btrfs)
            parser.reinit
          end
        end
      end
      btrfs.register_filefrag_speed(count: files.size,
                                    time: Time.now - io_start,
                                    cached: cached)
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
    include Outputs

    # Should be enough: we don't expect to defragment more than 1/s
    # there's an hardcoded limit of 24 in position_offset
    ENTRIES_INDEX_BITS = 16
    MAX_ENTRIES = 2 ** ENTRIES_INDEX_BITS
    ENTRIES_MASK = MAX_ENTRIES - 1
    MAX_ENTRY_VALUE = 2 ** 8 - 1
    # How often a file is changing its hash data source to avoid colliding
    # with the same other files
    ROTATING_PERIOD = 7 * 24 * 3600 # one week
    # Split the objects in smaller groups that rotate independently
    # to avoid spikes on rotations (this is chosen for a partial rotation
    # occuring frequently : every 604800 / 65536 ~= 9 seconds which can't
    # create large spikes)
    # We use bits to choose them with a mask instead of a modulo (faster and
    # in a heavily used code)
    ROTATE_GROUP_BITS = 16
    ROTATE_GROUPS = 2 ** ROTATE_GROUP_BITS
    ROTATE_GROUP_MASK = ROTATE_GROUPS - 1
    # We extract the entry from a subset of a 64 bit integer so can't ignore
    # more than that
    MAX_DIGEST_IGNORE_BITS = 64 - ENTRIES_INDEX_BITS

    attr_reader :size

    def initialize(serialized_data = nil)
      @tick_interval = IGNORE_AFTER_DEFRAG_DELAY.to_f / MAX_ENTRY_VALUE
      # Reset recent data if rules changed or invalid serialization format
      if !serialized_data || !serialized_data["ttl"] ||
         (serialized_data["ttl"] > IGNORE_AFTER_DEFRAG_DELAY) ||
         (serialized_data["bitarray"].size != MAX_ENTRIES)
        dump = serialized_data && serialized_data.reject{|k,v| k == "bitarray"}
        info "Invalid serialized data: \n#{dump.inspect}"
        @bitarray = "\0".force_encoding(Encoding::ASCII_8BIT) * MAX_ENTRIES
        @last_tick = Time.now.to_f
        @size = 0
        return
      end
      @bitarray = serialized_data["bitarray"]
      # Old versions tried to serialize a Time object
      # Recent Ruby versions reject them so convert to avoid migration pains
      @last_tick = serialized_data["last_tick"].to_f
      @bitarray.force_encoding(Encoding::ASCII_8BIT)
      compute_size
    end

    # Expects a string
    # set value to max in the entry (0 will indicate entry has expired)
    def event(object_id)
      position = object_position(object_id)

      previous_value = @bitarray.getbyte(position)
      @bitarray.setbyte(position, MAX_ENTRY_VALUE)
      @size += 1 if previous_value == 0
    end

    def recent?(object_id)
      @bitarray.getbyte(object_position(object_id)) != 0
    end

    def serialization_data
      { # Dup is used for moving data because it isn't stored right away
        # and @last_tick is reassigned while @bitarray is changed in place
        "bitarray" => @bitarray.dup,
        "last_tick" => @last_tick,
        "ttl" => IGNORE_AFTER_DEFRAG_DELAY }
    end

    def tick_handler_for_async_runner
      advance_clock
      next_tick_at
    end

    def next_tick_at
      # Add a fraction of a second to avoid being scheduled too soon
      # should not be needed but costs almost nothing and clocks are tricky
      # AsyncRunner expects a Time object
      Time.at(@last_tick + @tick_interval + 0.001)
    end

    private

    def compute_size
      @size =
        @bitarray.each_byte.reduce(0) { |sum, byte| byte == 0 ? sum : sum + 1 }
    end

    # Advance the clock and test for abnormal conditions
    # TODO: remove these tests later
    def advance_clock
      now = Time.now.to_f
      if (@last_tick + @tick_interval) >= now
        error "** FuzzyEventTracker early tick scheduling"
        return
      end

      tick!
      if (@last_tick + @tick_interval) < now
        error "** FuzzyEventTracker missed some ticks:" \
              "#{@last_tick + @tick_interval} < #{now}"
      end
      tick! while (@last_tick + @tick_interval) < now
    end

    # Rewrite bitarray, decrementing each value and updating size
    def tick!
      @last_tick += @tick_interval
      return if @size == 0 # nothing to be done

      # There is a chance of a race condition for @size:
      # adding a new tracked event where there was none before it has been
      # counted in the loop below will increase @size twice, this will be fixed
      # on next tick! and is acceptable.
      # the byte value can be decremented while a concurrent tracked event
      # occurs ignoring the tracked event if it occurs between the bitarray read
      # and the write for the same offset.
      # This is extremely unlikely and thus acceptable too.
      @size = 0
      @bitarray.each_byte.with_index do |byte, idx|
        next if byte == 0 # should be common

        @bitarray.setbyte(idx, byte - 1)
        @size += 1 if byte > 1
      end
    end

    # The actual position for each object_id slowly changes over time
    # to avoid the unavoidable collisions of the FuzzyEventTracker
    # being fixed and preventing some files from being defragmented long-term.
    # The time of change varies for each object_id to avoid all collisions to
    # change at the same time, discovering many "hidden" fragmented files at
    # once (which could generate large spikes of defragmentation activity).
    def object_position(object_id)
      # We assume 64 bits of the MD5 digest is enough information
      # to avoid almost all collisions
      object_digest = Digest::MD5.digest(object_id)
      long_int, _long_int2 = object_digest.unpack('Q*')
      # choose the rotating group using a portion of the digest
      rotating_group = long_int & ROTATE_GROUP_MASK
      # Rotation :
      # - occurs every ROTATING_PERIOD
      # - is offset by the rotating_group
      rotation = ((Time.now.to_f / ROTATING_PERIOD) +
                  (rotating_group.to_f / ROTATE_GROUPS)).to_i
      # When rotating we use a different part of the digest by choosing
      # the amount of bits we ignore between 0 and the maximum possible
      # leaving enough data to cover MAX_ENTRIES
      ignored_bits = rotation % MAX_DIGEST_IGNORE_BITS
      # We get the final position by offsetting and selecting the low bits
      (long_int >> ignored_bits) & ENTRIES_MASK
    end
  end

  def initialize(btrfs)
    @btrfs = btrfs

    @fragmentation_info_mutex = Mutex.new
    # Queue of FileFragmentation to defragment, ordered by fragmentation cost
    @file_fragmentations = { compressed: [], uncompressed: [] }
    # The same, by their shortname, stored as { type: type, frag: frag } hashes
    @to_defrag_by_shortname = {}

    @last_queue_overflow_at = nil
    @fetch_accumulator = {
      compressed: 0.5,
      uncompressed: 0.5,
    }
    @type_tracker = {
      compressed: 1.0,
      uncompressed: 1.0,
    }
    @tracker_mutex = Mutex.new
    load_recently_defragmented
    load_cost_history

    @written_files = {}
    @writes_mutex = Mutex.new
    @to_filefrag = Queue.new
  end

  def status_at=(status_at)
    @status_at = status_at
  end

  def flush_all
    puts "= #{@btrfs.dirname}; flushing cost history"
    _serialize_history
    puts "= #{@btrfs.dirname}; flushing recent defragmentations"
    serialize_recently_defragmented
  end

  def any_interesting_file?
    @fragmentation_info_mutex.synchronize do
      TYPES.any? { |t| @file_fragmentations[t].any? }
    end
  end

  def queued
    @fragmentation_info_mutex.synchronize do
      @file_fragmentations.values.map(&:size).inject(&:+)
    end
  end

  def next_defrag_duration
    @fragmentation_info_mutex.synchronize do
      last_filefrag = @file_fragmentations[next_available_type].last
      return nil unless last_filefrag

      # We return the next chunk defrag duration if defrag by chunk is active
      file_size = last_filefrag.size
      if $chunk_size && file_size > $chunk_size
        last_filefrag.defrag_time * $chunk_size / file_size
      else
        last_filefrag.defrag_time
      end
    end
  end

  # Implement a weighted round-robin
  def pop_most_interesting
    @fragmentation_info_mutex.synchronize do
      # Choose type before updating the accumulators to avoid inadvertedly
      # switching to another type after calling next_defrag_duration
      current_type = next_available_type
      @fetch_accumulator[current_type] = @fetch_accumulator[current_type] % 1.0
      TYPES.each { |type| @fetch_accumulator[type] += type_share(type) }
      frag = @file_fragmentations[current_type].pop
      @to_defrag_by_shortname.delete(frag.short_filename)
      frag
    end
  end

  # Not thread-safe but acceptable (rare false positives would be OK)
  # In reality as we use it to find out if we should track a file
  # and we remove tracking *after* marking a file defragmented, this ensures
  # false positives don't have any consequence
  def recently_defragmented?(shortname)
    @recently_defragmented.recent?(shortname)
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
    @recently_defragmented.event(shortname)
    serialize_recently_defragmented if must_serialize_recent?
    remove_tracking(shortname)
  end

  def remove_tracking(shortname)
    @writes_mutex.synchronize { @written_files.delete(shortname) }
  end

  def tracking_writes?(shortname)
    @writes_mutex.synchronize { @written_files.include?(shortname) }
  end

  # This adds or updates work to do based on fragmentations
  # returns the number of new files put in queue
  # This can be called relatively often (multiple times per second per fs),
  # handles multi-thousands item lists and locks a mutex for its duration:
  # some care went into making this fast
  def select_for_defragmentation(file_fragmentations)
    deleted_frags = Set.new
    to_add_frags = {}
    @fragmentation_info_mutex.synchronize do
      # Process latest frags first (to allow ignoring older duplicates)
      file_fragmentations.reverse_each do |frag|
        shortname = frag.short_filename
        unless below_threshold_cost(frag)
          # Don't add the same file twice
          to_add_frags[shortname] ||= { type: frag.compress_type, frag: frag }
        end
        # Do we have an entry to delete
        old_entry = @to_defrag_by_shortname.delete(shortname)
        # Did we detect an entry to delete (ignore duplicates)?
        next unless old_entry && !deleted_frags.include?(shortname)

        deleted_frags << shortname
        @file_fragmentations[old_entry[:type]].delete(old_entry[:frag])
      end
      # No need to add/sort/trim if we only deleted entries
      # This is the common case when walking an already processed fs
      return 0 if to_add_frags.empty?

      to_add_frags.each_value do |hash|
        @file_fragmentations[hash[:type]] << hash[:frag]
      end
      @to_defrag_by_shortname.merge!(to_add_frags)
      # Keep the order to easily fetch the worst fragmented ones
      sort_frags
      # Remove old entries and fit in max queue length
      trim_frags
    end
    to_add_frags.size
  end

  def file_written_to(filename)
    shortname = @btrfs.short_filename(filename)
    return if recently_defragmented?(shortname)

    write_events = @written_files[shortname]
    if write_events
      # Don't waste time acquiring a Mutex here, if this entry is deleted
      # concurrently there won't be any adverse effect
      write_events.write!
    else
      @writes_mutex.synchronize { @written_files[shortname] = WriteEvents.new }
    end
  end

  def historize_cost_achievement(type, initial_cost, final_cost, size)
    @cost_achievement_history[type] <<
      [ initial_cost, final_cost, size, Time.now.to_i ]
    serialize_history
  end

  def average_cost(type)
    @average_costs[type]
  end

  def below_threshold_cost(frag)
    frag.fragmentation_cost <= threshold_cost(frag)
  end

  # We are tracking the relative amount of (un)compressed files
  # to know at which relative rates we should fetch them from the queue
  def type_track(types)
    @tracker_mutex.synchronize do
      types.each { |type| @type_tracker[type] += 1.0 }
      total = @type_tracker.values.inject(&:+)
      memory = 10_000.0 # TODO: tune/config
      break if total <= memory

      @type_tracker.each_key { |key| @type_tracker[key] *= (memory / total) }
    end
  end

  def type_share(type)
    @tracker_mutex.synchronize do
      total = @type_tracker.values.inject(&:+)
      # Return a default value if there isn't enough data, assume equal shares
      (total < 10) ? 0.5 : @type_tracker[type] / total
    end
  end

  def queue_fill_proportion
    @fragmentation_info_mutex.synchronize do
      total_queue_size.to_f / MAX_QUEUE_LENGTH
    end
  end

  def status
    last_compressed_cost, last_uncompressed_cost =
    @fragmentation_info_mutex.synchronize do
      first_compressed = @file_fragmentations[:compressed][0]
      first_uncompressed = @file_fragmentations[:uncompressed][0]
      [ first_compressed ? "%.2f" % first_compressed.fragmentation_cost :
          "none",
        first_uncompressed ? "%.2f" % first_uncompressed.fragmentation_cost :
          "none" ]
    end
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
    # Skip can happen (suspend for example)
    @status_at += STATUS_PERIOD while Time.now > @status_at
    @status_at
  end

  def consolidate_writes
    batch, min_last = @writes_mutex.synchronize do
      # used for each entry
      threshold = Time.now - STOPPED_WRITING_DELAY
      old_threshold = Time.now - MAX_WRITES_DELAY
      # Detect written files whose fragmentation should be checked
      candidates = @written_files.select do |shortname, value|
        (value.last < threshold) || (value.first < old_threshold)
      end.keys

      candidates.each { |shortname| @written_files.delete(shortname) }

      # Cleanup written_files if it overflows, moving files to the
      # defragmentation queue
      if @written_files.size > MAX_TRACKED_WRITTEN_FILES
        to_remove = @written_files.size - MAX_TRACKED_WRITTEN_FILES
        keys_to_remove = @written_files.keys.sort_by do |short|
          @written_files[short].last
        end[0...to_remove]
        keys_to_remove.each do |short|
          candidates << short
          @written_files.delete(short)
        end
        info("** %s writes tracking overflow: %d files queued for defrag" %
             [ @btrfs.dirname, to_remove ])
      end
      [ candidates, @written_files.values.map(&:last).min ]
    end

    # Use full filenames and filter deleted files, pass them
    # to the filefrag queue
    batch.map { |short| @btrfs.full_filename(short) }
         .select { |filename| File.file?(filename)}
         .each { |filename| @to_filefrag.push filename }
    # When should we restart ?
    if min_last
      [ min_last + STOPPED_WRITING_DELAY,
        Time.now + MIN_WRITTEN_FILES_LOOKUP_PERIOD ].max
    else
      # Nothing to process yet: this is the earliest we can have work to do
      Time.now + STOPPED_WRITING_DELAY
    end
  end

  def filefrag_loop
    loop do
      # This one is there to block waiting for input without wasting resources
      list = []
      list << @to_filefrag.pop
      # But we want to process everything in a batch
      list << @to_filefrag.pop until @to_filefrag.empty?
      # Duplicates might be possible under heavy load
      list.uniq!
      select_for_defragmentation(FileFragmentation.create(list, @btrfs,
                                                          cached: true))
      # Under heavy load @to_filefrag can grow faster than
      # FileFragmentation.create can process new files
      skipped = 0
      while @to_filefrag.size > MAX_FILEFRAG_QUEUE_SIZE
        # Ignore oldest
        @to_filefrag.pop
        skipped += 1
      end
      next unless skipped > 0

      info("** %s: waiting_filefrag queue overflow, skipped %d" %
           [ @btrfs.dirname, skipped ])
    end
  end

  # each achievement is [ initial_cost, final_cost, file_size ]
  # file_size is currently ignored (the target was jumping around on filesystems
  # with very diverse file sizes)
  def compute_thresholds
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
  end

  def next_event_tracker_tick_at
    @recently_defragmented.next_tick_at
  end

  def event_tracker_tick_handler_for_async_runner
    @recently_defragmented.tick_handler_for_async_runner
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

  # The threshold can be raised so high by a succession of difficult files
  # that it would stop any defragmentation from happening and block the
  # threshold itself
  # Adapting based on the defragmentation rate safeguards against this
  def normalize_cost_threshold(cost)
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
    _serialize_history
    @last_history_serialized_at = Time.now
  end
  def _serialize_history
    serialize_entry(HISTORY_STORE, @btrfs.dir, @cost_achievement_history)
  end
  def load_cost_history
    default_value = { compressed: [], uncompressed: [] }
    @cost_achievement_history = unserialize_entry(HISTORY_STORE, @btrfs.dir,
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
      FuzzyEventTracker.new(unserialize_entry(RECENT_STORE, @btrfs.dir,
                                              "recently defragmented"))
    @last_recent_serialized_at = Time.now
  end
  # Note: must be called protected by @fragmentation_info_mutex
  def serialize_recently_defragmented
    serialize_entry(RECENT_STORE, @btrfs.dir,
                    @recently_defragmented.serialization_data)
    @last_recent_serialized_at = Time.now
  end

  def sort_frags
    TYPES.each { |t| @file_fragmentations[t].sort_by!(&:fragmentation_cost) }
  end

  # Must be called protected by @fragmentation_info_mutex and after sorting
  def trim_frags
    return unless total_queue_size > MAX_QUEUE_LENGTH

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
    trim_type_to(:compressed, compressed_target_size)
    trim_type_to(:uncompressed, uncompressed_target_size)
  end
  def trim_type_to(type, target)
    current_size = queue_size(type)
    return unless current_size > target

    start = current_size - target
    @file_fragmentations[type][0...start].each do |frag|
      @to_defrag_by_shortname.delete(frag.short_filename)
    end
    @file_fragmentations[type] = @file_fragmentations[type][start..-1]
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
  include Delaying
  include AlgebraUtils

  # create the internal structures, including references to other mountpoints
  def initialize(dir, dev_fs_map, fs_dev_map)
    @dir = dir
    @dir_slash = dir.end_with?("/") ? dir : "#{dir}/"
    @dirname = File.basename(dir)
    @checker = UsagePolicyChecker.new(self)
    @files_state = FilesState.new(self)
    # Note: @files_state MUST be created before @rate_controller
    @rate_controller = FilesStateRateController.new(dev: self)

    # Tracking of file defragmentation
    @files_in_defragmentation = {}
    @perf_queue = Queue.new

    # Init filefrag time tracking and rate with sensible values
    @average_file_time = { cached: 0, not_cached: 0 }
    detect_options(dev_fs_map, fs_dev_map)
  end

  def average_file_time(cached:)
    @average_file_time[cached_key(cached)]
  end

  def detect_options(dev_fs_map, fs_dev_map)
    @my_dev_id = File.stat(dir).dev
    update_subvol_dirs(dev_fs_map)
    load_exceptions(fs_dev_map)
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

  def active_for_defragmentation?
    !autodefrag?
  end

  def start_processing
    # Pre-init these (used by slow_status and slow_files_state_update
    @already_processed = @recent = @queued = 0
    @slow_scan_thread = Thread.new do
      info("## Beginning files list updater thread for #{dir}")
      loop do
        slow_files_state_update
        defragment_extent_and_subvolume_trees if $defragment_trees
      end
    end
    @fragmentation_updater_thread = Thread.new { @files_state.filefrag_loop }
    @perf_queue_thread = Thread.new { handle_perf_queue_progress }

    # wait for init_pass_speed (@files_state.status relies on it)
    sleep 1 while (@rate_controller && @rate_controller.current_batch_size).nil?
    @files_state.status_at = @slow_status_at = Time.now

    @runner = AsyncRunner.new(dirname)
    @runner.add_task(name: "slow scan status") { slow_status }
    @runner.add_task(name: "status") { @files_state.status }
    @runner.add_task(name: "fragmentation thresholds") do
      @files_state.compute_thresholds
      Time.now + COST_COMPUTE_DELAY
    end
    @runner.add_task(name: "usage policy checker cleanup") do
      @checker.cleanup
      Time.now + DEVICE_LOAD_WINDOW
    end
    @runner.add_task(name: "fuzzy event tracker ticks",
                     time: @files_state.next_event_tracker_tick_at ) do
      @files_state.event_tracker_tick_handler_for_async_runner
    end
    @async_thread = Thread.new { @runner.run }

    @io_runner = AsyncRunner.new(dirname)
    @io_runner.add_task(name: "defragmenter") do
      defrag!
      available_for_defrag_at
    end
    @io_runner.add_task(name: "consolidate_writes") do
      @files_state.consolidate_writes
    end
    @io_async_thread = Thread.new { @io_runner.run }
  end

  def stop_processing
    # This is the right time to store our state
    flush_all
    [ @slow_scan_thread, @async_thread, @io_async_thread, @perf_queue_thread,
      @fragmentation_updater_thread ].each do |thread|
      Thread.kill(thread) if thread
    end
    @io_runner = nil
    @runner = nil
  end

  def dump_timings
    # compact to handle stopped case
    [ @io_runner, @runner ].compact.each(&:dump_timings)
  end

  def has_dev?(dev_id)
    @dev_list.include?(dev_id)
  end

  # Manage queue of asynchronously defragmented files
  def queue_defragmentation_performance_check(file_frag)
    @perf_queue.push({ file_frag: file_frag,
                       queued_at: Time.now,
                       start_cost: file_frag.fragmentation_cost,
                       size: file_frag.size })
  end

  def handle_perf_queue_progress
    loop do
      value = @perf_queue.pop
      delay_until(value[:queued_at] + commit_delay)
      file_frag = value[:file_frag]
      file_frag.update_fragmentation
      # Don't consider fragmentation increases (might be concurrent writes)
      new_fragmentation =
          [ value[:start_cost], file_frag.fragmentation_cost ].min
      @files_state.historize_cost_achievement(file_frag.compress_type,
                                              value[:start_cost],
                                              new_fragmentation,
                                              value[:size])
      skipped = 0
      while @perf_queue.size > MAX_PERF_QUEUE_SIZE
        @perf_queue.pop
        skipped += 1
      end
      info("** %s: perf queue overflow, skipping %d" %
           [ @dirname, skipped ]) if skipped > 0
    end
  end

  def claim_file_write(filename)
    # local_name: name in the main tree
    # (fatrace could detect them in other mountpoints)
    local_name = position_on_fs(filename)
    return false unless local_name
    @files_state.file_written_to(local_name) unless skip_defrag?(local_name)
    true
  end

  def handle_file_write(filename)
    @files_state.file_written_to(filename) unless skip_defrag?(filename)
  end

  def position_on_fs(filename)
    return filename if filename.start_with?(@dir_slash)

    subvol_fs = @fs_map.keys.find { |fs| filename.start_with?(fs) }
    return nil unless subvol_fs

    # This is a local file, triggered from another subdir, move in our domain
    filename.gsub(subvol_fs, @fs_map[subvol_fs])
  end

  def each_fs_map
    @fs_map.each_pair { |path, subvol| yield(path, subvol) }
  end

  def defrag!
    file_frag = get_next_file_to_defrag
    return unless file_frag

    # To avoid long runs without status if there are files to defragment
    shortname = file_frag.short_filename
    # We declare it defragmented ASAP to avoid a double queue
    @files_state.defragmented!(shortname)
    filename = file_frag.filename
    size = file_frag.size
    verbose do
      # Deal with file deletion race condition
      mtime = File.mtime(filename) rescue nil
      " - %s: %s %s,%s,%.2f,%s" %
        [ dir, shortname, (file_frag.majority_compressed? ? "C" : "U"),
          human_size(size), file_frag.fragmentation_cost,
          human_delay_since(mtime) ]
    end

    if $chunk_size && size > $chunk_size
      start = 0
      start_at = Time.now if $verbose
      next_chunk_size = $chunk_size
      defrag_time = file_frag.defrag_time
      loop do
        run_with_device_usage do
          system(*defrag_cmd, '-s', start.to_s, '-l', next_chunk_size.to_s,
                 filename)
        end
        start += $chunk_size
        break if start >= size

        next_chunk_size = [ size - start, $chunk_size ].min
        expected_chunk_defrag_duration = defrag_time * next_chunk_size / size
        delay_until(available_for_defrag_at(expected_chunk_defrag_duration))
      end
      verbose do
        " - %s: %s LAST CHUNK, %s" % [ dir, shortname,
                                       human_duration(Time.now - start_at) ]
      end
    else
      run_with_device_usage { system(*defrag_cmd, filename) }
    end
    queue_defragmentation_performance_check(file_frag)
  end

  # Experimental, the impact of this isn't depicted in the BTRFS documentation
  # when defragmenting directories without -r we defragment additional
  # structures: extent and subvolume trees. Can destroy performance on HDD
  def defragment_extent_and_subvolume_trees
    # Defragment root subvolume
    verbose { " - #{dir}: root subvolume extent and subvolume trees" }
    run_with_device_usage { system(*defrag_cmd, dir) }
    verbose { " - #{dir}: root subvolume trees done" }
    @rw_subvols.each do |subvol|
      # Let other threads have an opportunity to work as these are blocking
      # Thread.pass should be enough but it doesn't offer any guarantee
      Thread.pass; sleep 0.1
      verbose { " - #{dir}: #{subvol} extent and subvolume trees" }
      run_with_device_usage { system(*defrag_cmd, subvol) }
      verbose { " - #{dir}: #{subvol} trees done" }
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
      [ "btrfs", "filesystem", "defragment", "-t", $extent_size.to_s, "-f" ]
    cmd << "-c#{comp_algo_param_value}" if @compression_algo
    @defrag_cmd = cmd
  end

  def comp_algo_param_value
    @compression_algo.gsub(/:.*$/, '')
  end

  # We prefer to store short filenames to free memory
  def short_filename(filename)
    filename.gsub(@dir_slash, "")
  end
  def full_filename(short_filename)
    "#{@dir_slash}#{short_filename}"
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

  def available_for_filefrag_at(filecount, cached:)
    @checker.available_at(expected_time:
                            average_file_time(cached: cached) * filecount)
  end

  def scan_status
    speed_factor = queue_speed_factor
    delay_speedup = @rate_controller.speedup_due_to_scan_delay
    adjustement = ""
    # No need to display speed adjustements when there aren't any
    if speed_factor <= 0.99 || delay_speedup >= 1.01
      adjustement = " adj "
      msgs = []
      msgs << ("q&l:%.2f" % speed_factor) if speed_factor <= 0.99
      msgs << ("d:%.2f" % delay_speedup) if delay_speedup >= 1.01
      adjustement << msgs.join(',')
    end
    "%d/%.3fs (%.3fs expected, IO %.0f%%) %s%s" %
      [ @rate_controller.current_batch_size,
        @rate_controller.current_batch_period, average_batch_time,
        @checker.load * 100, @rate_controller.relative_scan_speed_description,
        adjustement ]
  end

  def register_filefrag_speed(count:, time:, cached:)
    # Divide memory by count as we basically take in count new values
    memory = (2 * MAX_FILES_BATCH_SIZE).to_f / count
    key = cached_key(cached)
    @average_file_time[key] =
      memory_avg(@average_file_time[key], memory, time / count)
  end

  # Adjust filefrag call speed based on amount of queued files
  # We slow the scan above QUEUE_PROPORTION_EQUILIBRIUM
  # Speedup is handled by speedup_due_to_scan_delay
  def queue_speed_factor
    queue_proportion = @files_state.queue_fill_proportion
    factor = if queue_proportion > QUEUE_PROPORTION_EQUILIBRIUM
               # linear scale between 1 and SLOW_SCAN_MIN_SPEED_FACTOR
               scaling(queue_proportion, QUEUE_PROPORTION_EQUILIBRIUM..1,
                       1..SLOW_SCAN_MIN_SPEED_FACTOR)
             else
               1
             end
    factor / LoadCheck.instance.slowdown_ratio
  end

  private

  def cached_key(cached)
    cached ? :cached : :not_cached
  end

  # This class handles batch processing, it expects
  # - a FilesStateRateController (used for batch size),
  # - a block taking a files list to call when the batch is ready
  class Batch
    attr_reader :filelist

    def initialize(rate_controller:, &block)
      @rate_controller = rate_controller
      @block = block
      reset
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
      ((@filelist.size + @ignored) >= @rate_controller.current_batch_size) ||
        (@arg_length >= FILEFRAG_ARG_MAX)
    end

    private

    def reset
      @filelist = []
      @arg_length = 0
      @ignored = 0
    end

    def handle_full_batch
      return unless ready?

      @block.call(filelist)
      reset
      @rate_controller.set_current_batch_target
      @rate_controller.wait_next_slow_scan_pass
    end
  end

  class FilesStateRateController
    attr_accessor :target_stop_time, :considered
    attr :current_batch_size, :current_batch_period
    include HashEntrySerializer
    include HumanFormat
    include Outputs
    include Delaying
    include AlgebraUtils

    def initialize(dev:)
      @pass = :first
      @caught_up = false
      @dev = dev
      load_filecount
      # Need to define default values until init_new_scan (some other threads
      # ask us about current rate status)
      @target_stop_time = Time.now + SLOW_SCAN_PERIOD
      # Set sensible values (we can log these before the scan actually starts)
      # will be appropriately computed again
      init_pass_speed
    end

    def relative_scan_speed_description
      if @filecount.nil? || @filecount == 0
        "unknow"
      elsif scan_time == 0
        "init"
      else
        "%.1f%%" % (100 * (considered.to_f / @filecount) /
                    (scan_time.to_f / SLOW_SCAN_PERIOD)).to_f
      end
    end

    # Return a speed factor increase to compensate for past delays
    def speedup_due_to_scan_delay
      # Don't have useful data to set a target speed
      return 1 unless @filecount

      target_considered = (scan_time.to_f / SLOW_SCAN_PERIOD) * @filecount
      filecount_delay = target_considered - considered
      return 1 if filecount_delay <= 0

      [ 1 + (SLOW_SCAN_MAX_SPEED_FACTOR - 1) *
            (filecount_delay / (SLOW_SCAN_MAX_SPEED_AT * @filecount)),
        SLOW_SCAN_MAX_SPEED_FACTOR ].min
    end

    def set_current_batch_target
      @current_batch_size, @current_batch_period = speed_to_batch(current_speed)
    end

    def init_new_scan
      @scan_start = Time.now
      if @pass == :first
        info("= #{@dev.dirname}: skipping #{@processed} files " \
             "in #{SLOW_SCAN_CATCHUP_WAIT}s")
        # Avoid IO load just after boot (see "--slow-start" option)
        @scan_start += SLOW_SCAN_CATCHUP_WAIT
        @pass = :non_first
      end
      this_start = @scan_start
      # If we have to skip some files, skip the corresponding time period
      if @filecount && @filecount > 0 && @processed > 0
        # compute an approximate time start from the work already done
        adjustement = if @processed <= @filecount
                        SLOW_SCAN_PERIOD * @processed.to_f / @filecount
                      else
                        # processing accelerates after @filecount
                        SLOW_SCAN_PERIOD + time_exceeded_for_processed
                      end
        @scan_start -= adjustement
      end
      @target_stop_time = @scan_start + SLOW_SCAN_PERIOD
      init_pass_speed
      delay_until this_start
      @last_slow_scan_batch_start = Time.now
    end

    def wait_next_slow_scan_pass
      update_filecount(processed: considered)
      delay_until(@last_slow_scan_batch_start + @current_batch_period)
      @last_slow_scan_batch_start = Time.now
    end

    def catching_up?
      return false if @caught_up
      return true if considered < @processed

      info "= #{@dev.dirname}: caught up #{@processed} files"
      @caught_up = true
      false
    end

    def update_filecount_on_scan_end
      update_filecount(processed: 0, total: considered)
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
      serialize_filecount
      @last_filecount_updated_at = now
      debug { "# #{@dev.dirname}, #{entry.inspect}" } if total
    end

    def wait_slow_scan_restart
      # If @filecount.nil? we were counting files, restart fast
      return unless @filecount && scan_time_left > 0

      delay [ scan_time_left, MAX_DELAY_BETWEEN_SLOW_SCANS ].min
    end

    def scan_time
      return 0 unless @scan_start

      Time.now - @scan_start
    end

    def flush_all
      puts "= #{@dev.dirname}; flushing file count progress"
      serialize_filecount
    end

    private

    def current_speed
      # Make a fast first pass if we don't know the filesystem yet
      # (rework for large and busy filesystems ?)
      return max_speed * @dev.queue_speed_factor unless @filecount

      # If there isn't enough time or data, speed up
      return catching_up_speed if Time.now > @target_stop_time

      ## Adaptive speed during normal scan
      @pass_target_speed * @dev.queue_speed_factor * speedup_due_to_scan_delay
    end

    def serialize_filecount
      entry = { processed: @processed, total: @filecount }
      serialize_entry(FILE_COUNT_STORE, @dev.dir, entry)
    end

    def scan_time_left
      @target_stop_time - Time.now
    end

    def load_filecount
      default_value = { processed: 0, total: nil }
      entry = unserialize_entry(FILE_COUNT_STORE, @dev.dir, "filecount",
                                default_value)
      @processed = entry[:processed]
      @filecount = entry[:total]
    end

    # Get filecount or a default value (min 1 to avoid divide by 0)
    def expected_filecount
      [ @filecount || (SLOW_SCAN_PERIOD * max_speed), 1 ].max
    end

    # Called if @processed > @filecount, processing accelerates exponentially
    # after @filecount (see catching_up_speed) we reproduce it there
    # and scale back the acceleration (to avoid too much scan IO after restart)
    def time_exceeded_for_processed
      increments = 0
      processed = 0
      target = @processed - @filecount
      loop do
        processed += @pass_target_speed *
                     (SLOW_SCAN_SPEED_INCREASE_STEP ** increments) *
                     scan_speed_increase_period
        break if processed > target
        increments += 1
      end
      # Reduce the load artificially
      (increments * scan_speed_increase_period) / 2
    end

    def max_speed
      MAX_FILES_BATCH_SIZE.to_f / MIN_DELAY_BETWEEN_FILEFRAGS
    end

    def init_pass_speed
      @speed_increases = 0
      @considered = 0
      @pass_target_speed = expected_filecount.to_f / SLOW_SCAN_PERIOD
      @current_batch_size, @current_batch_period =
                           speed_to_batch(@pass_target_speed)
    end

    # When we have exceeded the scan time, speedup exponentially with time
    # use the queue_speed_factor to slow down temporarily when needed
    def catching_up_speed
      overshoot = Time.now - @target_stop_time
      speedup = false
      while @speed_increases < (overshoot / scan_speed_increase_period)
        @speed_increases += 1
        # We increase the speed by SLOW_SCAN_SPEED_INCREASE_STEP each time
        speedup = true
      end
      adjustment = SLOW_SCAN_SPEED_INCREASE_STEP ** @speed_increases
      if speedup
        info("= #{@dev.dirname}: speedup scan to catch up (x%.2f) for next %s" %
             [ adjustment, human_duration(scan_speed_increase_period) ])
      end
      # Base the speed on the pass target speed computed at the pass beginning
      @pass_target_speed * adjustement * @dev.queue_speed_factor
    end

    # Compute a [size,delay] for batches enforcing available ranges
    # prefer batches of 1 if while batches occur less frequently than default
    # delay between batches, then increase batch size, then reduce delay
    def speed_to_batch(speed)
      # Speed is slow, use 1 for batch_size
      if speed <= DEFAULT_DELAY_BETWEEN_FILEFRAGS
        [ 1, [ 1 / speed, MAX_DELAY_BETWEEN_FILEFRAGS ].min ]
      # Raise batch_size to match speed until it reaches MAX_FILES_BATCH_SIZE
      elsif speed <= MAX_FILEFRAG_SPEED_WITH_DEFAULT_DELAY
        # size must be an Integer
        batch_size = (DEFAULT_DELAY_BETWEEN_FILEFRAGS * speed).ceil
        # Adjust period to actual batch_size
        [ batch_size, batch_size / speed ]
      # Then lower period to match speed down to min allowable period
      else
        [ MAX_FILES_BATCH_SIZE,
          [ MAX_FILES_BATCH_SIZE / speed, MIN_DELAY_BETWEEN_FILEFRAGS ].max ]
      end
    end

    # We want to reach max filefrag speed in SLOW_BATCH_PERIOD / 2 if we spend
    # more than SLOW_BATCH_PERIOD to finish the filesystem slow scan
    def scan_speed_increase_period
      # This can't change, cache it
      return @scan_speed_increase_period if @scan_speed_increase_period
      max_speed_ratio =
        (MAX_DELAY_BETWEEN_FILEFRAGS.to_f / MIN_DELAY_BETWEEN_FILEFRAGS) *
        MAX_FILES_BATCH_SIZE.to_f
      logstep = Math.log(SLOW_SCAN_SPEED_INCREASE_STEP)
      logratio = Math.log(max_speed_ratio)
      steps_needed = logratio / logstep
      @scan_speed_increase_period = (SLOW_SCAN_PERIOD / 2) / steps_needed
    end
  end

  # Slowly update files, targeting a SLOW_SCAN_PERIOD period for all updates
  def slow_files_state_update
    @rate_controller.init_new_scan
    @already_processed = @recent = @queued = 0
    @batch = Batch.new(rate_controller: @rate_controller) do |list|
               queue_slow_scan_batch(list)
             end
    begin
      Find.find(dir) do |path|
        (Find.prune; next) if prune?(path)

        # Don't process during a resume (don't try to skip based
        # on file type either to avoid IO load)
        if @rate_controller.catching_up?
          @rate_controller.considered += 1
          next
        end

        # ignore files with unparsable names
        short_name = short_filename(path) rescue ""
        next if short_name == ""
        stat = begin
                 File.lstat(path)
               rescue => ex
                 debug "- #{@dirname} #{path} removed, #{ex.class}: #{ex}"
                 next
               end
        # Only process file entries
        next unless stat.file?
        @rate_controller.considered += 1

        # Ignore recently processed files
        if @files_state.recently_defragmented?(short_name)
          @already_processed += 1
          @batch.add_ignored
          next
        end
        # Ignore tracked files
        if @files_state.tracking_writes?(short_name)
          @recent += 1; @batch.add_ignored
          next
        end
        # A file small enough to fit a node can't be fragmented
        # We don't count it as if nothing changes it won't become a target
        if stat.size <= 4096
          @rate_controller.considered -= 1
        else
          @batch.add_path(path)
        end
      end
    rescue => ex
      error("** Couldn't process #{dir}: " \
            "#{ex}\n#{ex.backtrace.join("\n")}")
      # Don't wait for a SLOW_SCAN_PERIOD but don't create load either
      @rate_controller.target_stop_time = Time.now + MAX_DELAY_BETWEEN_FILEFRAGS
    end
    # Process remaining files to update
    queue_slow_scan_batch(@batch.filelist)
    # Store the amount of files found
    @rate_controller.update_filecount_on_scan_end
    files_state_update_report
    @rate_controller.wait_slow_scan_restart
  end

  # Prune read-only subvolumes or blacklisted paths
  def prune?(entry)
    ro_subvol?(entry) || blacklisted?(entry)
  end

  def skip_defrag?(filename)
    (@autodefrag != false) || filename.end_with?(" (deleted)") ||
      blacklisted?(filename)
  end

  def mounted_with_compress?
    options = mount_options
    return nil unless options
    options.split(',').all? do |option|
      !(option.start_with?("compress=", "compress-force="))
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
      option.start_with?("compress=", "compress-force=")
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

  def queue_slow_scan_batch(filelist)
    # Note: this tracks filefrag speed and adjust batch size/period
    frags = FileFragmentation.create(filelist, self, cached: false)
    @queued += @files_state.select_for_defragmentation(frags)
  end

  def average_batch_time
    @average_file_time[:not_cached] * @rate_controller.current_batch_size
  end

  def slow_status
    now = Time.now
    defrag_rate =
      @files_state.defragmentation_rate(period: COST_THRESHOLD_TRUST_PERIOD)
    msg = ("$ %s %d/%ds: %d queued / %d found, " \
           "%d recent defrag (fuzzy), %d tracked, %.1f defrag/h") %
          [ @dirname, @rate_controller.scan_time.to_i, SLOW_SCAN_PERIOD,
            @queued, @rate_controller.considered, @already_processed, @recent,
            3600 * defrag_rate ]
    if @files_state.last_queue_overflow_at &&
       (@files_state.last_queue_overflow_at > (now - SLOW_SCAN_PERIOD))
      msg +=
        " ovf: %ds ago" % (now - @files_state.last_queue_overflow_at).to_i
    end
    info msg
    # This handles large slowdowns and suspends without spamming the log
    @slow_status_at += SLOW_STATUS_PERIOD until @slow_status_at > now
    @slow_status_at
  end

  def files_state_update_report
    info "$# %s full scan report" % @dirname
    slow_status
  end

  # Pass expected_duration when processing a file in chunks otherwise
  # this peeks at the next file to defragment
  def available_for_defrag_at(expected_duration = nil)
    expected_duration ||= @files_state.next_defrag_duration
    if expected_duration
      @checker.available_at(expected_time: expected_duration,
                            use_limit_factor: low_queue_defrag_speed_factor)
    else
      # avoid busy loop, this is the maximum wait available_at could return
      Time.now + DEVICE_LOAD_WINDOW
    end
  end

  # When the queue is low we slow down defrags to avoid spikes in IO activity
  # which don't help (as there's not much to do)
  def low_queue_defrag_speed_factor
    # At QUEUE_PROPORTION_EQUILIBRIUM we reach the max speed for defrags
    # But don't go below MIN_QUEUE_DEFRAG_SPEED_FACTOR
    [ [ @files_state.queue_fill_proportion / QUEUE_PROPORTION_EQUILIBRIUM,
        1 ].min, MIN_QUEUE_DEFRAG_SPEED_FACTOR ].max
  end

  def load_exceptions(fs_dev_map)
    no_defrag_list = []
    exceptions_file = "#{@dir_slash}#{DEFRAG_BLACKLIST_FILE}"
    if File.readable?(exceptions_file)
      no_defrag_list =
        File.read(exceptions_file).split("\n")
            .map { |path| "#{@dir_slash}#{path}" }
    end
    # Add mountpoints below our root that aren't a volume of the same FS
    # TODO: ignore mountpoints below another ignored mountpoints
    no_defrag_list += fs_dev_map.select do |fs, dev|
      (fs != dir) && fs.start_with?(@dir_slash) && (@my_dev_id != dev)
    end.keys

    if no_defrag_list.any? && (@no_defrag_list != no_defrag_list)
      info "= #{dir} blacklist: #{no_defrag_list.inspect}"
    end
    @no_defrag_list = no_defrag_list
  end

  def blacklisted?(filename)
    filename.start_with?(*@no_defrag_list)
  end

  def ro_subvol?(dir)
    @ro_subvols_set.include?(dir)
  end

  # This updates the subvolume list and tracks their mountpoints and
  # system device ids (for #known?)
  def update_subvol_dirs(dev_fs_map)
    subvol_dirs_list = BtrfsDev.subvolumes_by_writable(dir)
    @rw_subvols = subvol_dirs_list[true] || []
    @ro_subvols_set = (subvol_dirs_list[false] || []).to_set

    fs_map = {}
    dev_list = Set.new
    # We may not have '/' terminated paths everywhere but we must use them
    # for fast and accurate subpath detection/substitution in "position_on_fs"
    @rw_subvols.each do |subvol|
      full_path = normalize_path_slash(subvol)
      dev_id = File.stat(full_path).dev
      other_fs = dev_fs_map[dev_id]
      other_fs.each { |fs| fs_map[normalize_path_slash(fs)] = full_path }
      dev_list << dev_id
    end
    dev_list << File.stat(dir).dev
    if fs_map != @fs_map
      info "= #{dir}: changed filesystem maps"
      info fs_map.inspect
      @fs_map = fs_map
    end
    @dev_list = dev_list
  end

  def flush_all
    @files_state.flush_all
    @rate_controller.flush_all
  end

  def normalize_path_slash(dir)
    self.class.normalize_path_slash(dir)
  end

  class << self
    def list_subvolumes(dir)
      dir_slash = normalize_path_slash(dir)
      new_subdirs = []
      IO.popen([ "btrfs", "subvolume", "list", dir ]) do |io|
        while line = io.gets do
          line.chomp!
          if match = line.match(/^.* path (.*)$/)
            new_subdirs << "#{dir_slash}#{match[1]}"
          else
            error "** can't parse #{line}"
          end
        end
      end
      new_subdirs
    end

    def subvolumes_by_writable(dir)
      list_subvolumes(dir).group_by { |sub| File.writable?(sub) }
    end

    def normalize_path_slash(dir)
      return dir if dir.end_with?("/")

      "#{dir}/"
    end
  end
end

class BtrfsDevs
  include Outputs

  def initialize
    @btrfs_devs = []
    @new_fs = false
    # Used to find which dev handles which tree
    @dev_tree = {}
  end

  def flush_and_stop_all
    @btrfs_devs.each(&:stop_processing)
  end

  def dump_timings
    @btrfs_devs.each(&:dump_timings)
  end

  def fatrace_file_writes
    cmd = [ "fatrace", "-f", "W" ]
    extract_write_re = /\A[^(]+\([0-9]+\): [ORWC]+ (.*)\Z/
    loop do
      info("= (Re-)starting global fatrace thread")
      begin
        next_popen_at = Time.now + FATRACE_TTL
        IO.popen(cmd) do |io|
          begin
            while line = io.gets do
              # Skip btrfs commands (defrag mostly)
              next if line.start_with?("btrfs(")
              # TODO: maybe detect close syscalls to avoid timeouting
              # WriteEvents ?
              if extract_write_re =~ line
                handle_file_write($1)
              else
                error "** Can't extract file from '#{line}'"
              end
              # TODO: Maybe don't check on each pass (benchmark this)
              break if Time.now > next_popen_at
              # Fatrace should be able to detect writes on newly mounted fs
              # that said bugs where encountered, so this forces a restart
              break if recent_new_fs?
            end
          rescue => ex
            msg = "#{ex}\n#{ex.backtrace.join("\n")}"
            error "** Error in inner fatrace thread: #{msg}"
            sleep 1 # Limit CPU load in case of bug
          end
        end
      rescue => ex
        error "** Error in outer fatrace thread: #{ex}"
        delay = 60
        # Arbitrary sleep to avoid CPU load in case of fatrace repeated failure
        info("= Fatrace thread waiting #{delay}s before restart")
        sleep delay
      end
    end
  end

  def update!(detect_new_fs: true)
    # Enumerate BTRFS filesystems, build :
    # - a dir -> option_string map
    dirs = File.open("/proc/mounts", "r") do |f|
      f.readlines.map { |line| line.split(' ') }
        .select { |ary| ary[2] == 'btrfs' }
    end.map { |ary| [ ary[1], ary[3] ] }
    # - a deviceid -> [ dir1, dir2, ...] map
    dev_fs_map = Hash.new { |hash, key| hash[key] = Set.new }
    dirs.each { |dir, _options| dev_fs_map[File.stat(dir).dev] << dir }

    # Build a mountpoint to dev map to detect nested mountpoints from
    # other devs to ignore
    allmounts = File.open("/proc/mounts", "r") do |f|
      f.readlines.map { |line| line.split(' ') }
    end.map { |ary| ary[1] }
    fs_dev_map = allmounts.map do |dir|
      # Note: rescue nil is for handling a case where root can't access
      # a mount point (seems like a mostly harmless bug with tmpfs)
      dev_id = File.stat(dir).dev rescue nil
      [ dir, dev_id ]
    end.to_h

    umounted =
      @btrfs_devs.select { |dev| !dirs.map(&:first).include?(dev.dir) }
    umounted.each do |dev|
      info "= #{dev.dir} not mounted, disabling"
      dev.stop_processing
    end
    # @btrfs_devs.reject! { |dev| umounted.include?(dev) }
    still_mounted =
      @btrfs_devs.select { |dev| dirs.map(&:first).include?(dev.dir) }

    # Detect remount -o compress=... events and (no)autodefrag
    still_mounted.each { |dev| dev.detect_options(dev_fs_map, fs_dev_map) }
    newly_mounted = []
    dirs.map(&:first).each do |dir|
      next if known?(dir)
      next if still_mounted.map(&:dir).include?(dir)
      # More costly, tested last
      next unless top_volume?(dir)
      newly_mounted << BtrfsDev.new(dir, dev_fs_map, fs_dev_map)
      @new_fs = true if detect_new_fs
    end
    new_devs = still_mounted + newly_mounted
    # Longer devs first to avoid a top dir matching a file
    # in a device mounted below when using handle_file_write_old
    # new_devs.sort_by! { |dev| -dev.dir.size }
    @btrfs_devs = new_devs
    rebuild_dev_tree
  end

  # Note: there is a bug with this as it doesn't consider other types of
  # filesystems that could be mounted below a BTRFS mountpoint
  # it is probably a rare case and can be solved by adding entries for
  # these other mountpoints with dev: nil to be ignored by handle_file_write
  def rebuild_dev_tree
    tree = {}
    @btrfs_devs.each do |btrfs|
      next unless btrfs.active_for_defragmentation?
      # Add our root
      add_path_to_tree(tree: tree, path: btrfs.dir, dev: btrfs)
      # Add mappings from other mounts
      btrfs.each_fs_map do |path, subvol|
        add_path_to_tree(tree: tree, path: path, dev: btrfs, map: subvol)
      end
    end
    @dev_tree = tree
  end

  def handle_file_write(file)
    # Descend the tree until we reach the bottom
    # note that we don't access @dev_tree but its value, so @dev_tree can
    # be reaffected concurrently (no need for lock), but not modified in place
    current_tree = @dev_tree
    # / could be a btrfs filesystem and stored here
    dev = current_tree[:dev]
    path = current_tree[:path]
    map = current_tree[:map]
    list = file.split('/')
    # We already have information for '/', skip
    list.shift
    list.each do |dir|
      current_tree = current_tree[dir]
      # No subtree: our current values are the ones we are looking for, stop
      break unless current_tree

      current_path = current_tree[:path]
      # No current_path: this is an empty node, skip
      next unless current_path
      path = current_path
      dev = current_tree[:dev]
      map = current_tree[:map]
    end

    return unless dev
    # Rewrite the file location if we have a map
    dev.handle_file_write(map ? file.gsub(path, map) : file)
  end

  # We keep the old code as it is faster for low dev numbers
  # we might want to switch dynamically in future versions
  def handle_file_write_old(file)
    @btrfs_devs.find { |dev| dev.claim_file_write(file) }
  end

  # To call to detect a new fs being added to the watch list
  def recent_new_fs?
    # Using a copy and only resetting on true makes this thread-safe
    copy = @new_fs
    @new_fs = false if copy
    copy
  end

  def top_volume?(dir)
    dir_slash = BtrfsDev.normalize_path_slash(dir)
    BtrfsDev.list_subvolumes(dir_slash).all? do |subdir|
      Pathname.new(subdir).mountpoint?
    end
  end

  def known?(dir)
    dev_id = File.stat(dir).dev
    @btrfs_devs.any? { |btrfs_dev| btrfs_dev.has_dev?(dev_id) }
  rescue
    true # if File.stat failed, this is a race condition with concurrent umount
  end

  private

  def add_path_to_tree(tree:, path:, dev:, map: nil)
    current_subtree = tree
    Pathname.new(path).each_filename do |dir|
      current_subtree[dir] ||= {}
      current_subtree = current_subtree[dir]
    end
    current_subtree[:dev] = dev
    current_subtree[:path] = path
    current_subtree[:map] = map if map
  end
end

class Main
  include Outputs

  def initialize
    info "**********************************************"
    info "** Starting BTRFS defragmentation scheduler **"
    info "**********************************************"
    @devs = BtrfsDevs.new
  end

  def run
    # don't launch fatrace until devs are all accounted for
    # as it would trigger a fatrace restart
    @devs.update!(detect_new_fs: false)
    Thread.new { loop { sleep FS_DETECT_PERIOD; @devs.update! } }
    @devs.fatrace_file_writes
  end

  def flush_all_exit
    @devs.flush_and_stop_all
    AsyncSerializer.instance.stop_and_flush_all
    exit
  end

  def dump_timings
    @devs.dump_timings
  end
end

@main = Main.new

exit_interrupt_handler = proc { @main.flush_all_exit }
timings_interrupt_handler = proc { @main.dump_timings }
Signal.trap("INT", exit_interrupt_handler)
Signal.trap("TERM", exit_interrupt_handler)
Signal.trap("USR1", timings_interrupt_handler)

@main.run
