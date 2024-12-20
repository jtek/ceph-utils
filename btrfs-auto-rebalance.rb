#!/usr/bin/ruby -w

# Target when rebalancing (not much less than free_wasted_threshold)
TARGET_RATIO_FROM_THRESHOLD = 0.95
# THreshold for beginning raising the targets
USED_RATIO_FOR_MIN_TARGET = 0.5

MAX_FAILURES = 50
MAX_REBALANCES = 100
FLAPPING_LEVEL = 3
# How fast can we increase the -dusage/-musage targets
TARGET_INCREASE_STEP = 4

# Maximum time allocated globally
MAX_TIME = 14400 # 4 hours
MAX_FS_TIME = 5400 # 90 minutes


require 'optparse'

# spread value: meant to run daily, by default don't overlap next day's run
# waste target is adapted according to space used (less space: lower target)
@options = { min_waste_target: 0.35, max_waste_target: 0.5, verbose: true,
             only_analyze: false, spread: 24 * 3600 - MAX_TIME,
             stop_condition: '/bin/false' }
parser = OptionParser.new do |opts|
  opts.banner = <<EOS
Usage: btrfs-auto-rebalance.rb [options]
\twill balance filesytems until wasted allocation space (allocated
\tbut not used vs total allocated) reaches a target or too much time passed
EOS

  opts.on("-v", "--[no-]verbose", "Run verbosely", TrueClass) do |v|
    @options[:verbose] = v
  end
  opts.on("-t", "--min-waste-target WASTE_TARGET_ON_EMPTY",
          "used in combination with --max-waste-target, see below, default: " \
          "#{@options[:min_waste_target]})", Float) do |v|
    @options[:min_waste_target] = v
  end
  opts.on("-T", "--max-waste-target WASTE_TARGET_ON_FULL",
          "actual target progresses linearly between min and max when " \
          "used_ratio rises between #{USED_RATIO_FOR_MIN_TARGET} and 100%, " \
          "default: #{@options[:max_waste_target]})", Float) do |v|
    @options[:max_waste_target] = v
  end
  opts.on("-a", "--analyze-only",
          "Only compute waste spaces and display targets", TrueClass) do |v|
    @options[:only_analyze] = v
  end
  opts.on("-s", "--spread SECONDS", "Sleep randomly up to this amount",
          Integer) do |v|
    @options[:spread] = v
  end
  opts.on("-x", "--stop-condition CONDITION_CMD",
          "command which exit code determines if balance must stop", String) do |v|
    @options[:stop_condition] = v
  end
end
parser.parse!

if @options[:max_waste_target] < @options[:min_waste_target]
  puts parser.help
  puts
  puts "Error: max-waste-target < min-waste-target"
  exit 1
end

require 'open3'

def log(msg)
  STDERR.puts msg
  IO.popen("logger -p user.notice --id=$$", "w+") { |io| io.puts(msg) }
end

def filesystems
  mounts = []
  IO.popen("mount") do |io|
    io.each_line do |line|
      if match = line.match(/^(.*) on (.*) type btrfs/)
        # deduplicate
        next if mounts.map(&:first).include?(match[1])

        mounts << [ match[1], match[2] ]
      end
    end
  end
  mounts.map { |m| m[1] }
end

class Btrfs
  attr_reader :mountpoint

  def initialize(mountpoint, options)
    @mountpoint = mountpoint
    @options = options
    reset_tried_targets
    @flapping_detected = false
    @fs_start_time = nil
    refresh_usage
  end

  def tried(target)
    @tried_targets[target] += 1
    @flapping_detected = true if @tried_targets[target] == FLAPPING_LEVEL
  end

  def reset_tried_targets
    @tried_targets = Hash.new { 0 }
  end

  def refresh_usage
    IO.popen("btrfs fi usage --raw '#{@mountpoint}'") do |io|
      @device_slack = 0
      io.each_line do |line|
        case line
	when /Device size:\s*(\d*)$/
	  @device_size = Regexp.last_match[1].to_i
	when /Device slack:\s*(\d*)$/
	  @device_slack = Regexp.last_match[1].to_i
	when /Device allocated:\s*(\d*)$/
	  @allocated = Regexp.last_match[1].to_i
	when /Device unallocated:\s*(\d*)$/
	  @unallocated = Regexp.last_match[1].to_i
	when /Free \(estimated\):\s*(\d*)\s*\(min: \d*\)$/
	  @free = Regexp.last_match[1].to_i
	when /Data ratio:\s*(\d+\.\d+)$/
	  @ratio = Regexp.last_match[1].to_f
	end
      end
    end
  end

  def rebalance_if_needed
    if rebalance_needed?
      log_balance_start
      rebalance
    else
      log_current_state
    end
  rescue => ex
    log "rebalance error: #{ex}\n#{ex.backtrace.join("\n")}"
  end

  def log_balance_start
    log("#" * 80)
    log "%s: #{@mountpoint} rebalance started" %
        Time.now.strftime("%Y/%m/%d %H:%M:%S")
    log_current_state
  end

  def log_current_state
    log "%s: #{@mountpoint} current allocation state" %
        Time.now.strftime("%Y/%m/%d %H:%M:%S")
    log "\tused_ratio:  %.2f%%" % (used_ratio * 100)
    log "\twaste:       %.2f%%" % (free_wasted * 100)
    log "\tthreshold:   %.2f%%" % (free_wasted_threshold * 100)
    log "\tunallocated: #{@unallocated}"
    log "\tdata_ratio:  #{@ratio}"
    log "\tfree:        #{@free}"
  end

  def rebalance
    @fs_start_time = Time.now
    @working = true
    # Safeguard for long balances, will cancel a running balance if time expired
    setup_balance_cancel
    reset_tried_targets

    successive_failures = 0
    count = 0
    usage_target = start_target
    while (free_wasted > free_wasted_target) && (count < MAX_REBALANCES) &&
          (Time.now < must_stop_at)
      log "rebalance with usage: #{usage_target}"
      count += 1
      status = balance_usage(usage_target)
      refresh_usage
      if status != 0
        successive_failures += 1
        if usage_target == 0
	  fail "can't retry rebalancing, usage_target reached 0 already"
        end
	if successive_failures >= MAX_FAILURES
	  fail "too many rebalance failures: #{MAX_FAILURES}"
	end
        usage_target -= 1
      else
        successive_failures = 0
        usage_target = [ usage_target + TARGET_INCREASE_STEP, 100 ].min
      end
      log("%s: waste %.2f%%" % [ Time.now.strftime("%Y/%m/%d %H:%M:%S"),
                                 free_wasted * 100 ])
      if @flapping_detected
        fail "flapping detected: #{FLAPPING_LEVEL} times at this usage level"
      end
      next unless stop_condition

      fail "stop condition (#{@options[:stop_condition]}) reached"
    end
    if free_wasted > free_wasted_target
      log "#{free_wasted_target} not reached in #{MAX_REBALANCES} balance calls"
    end
  ensure
    # If we finished before our allocated time we must kill the cancel process
    log "%s: #{@mountpoint} rebalance stopped" %
        Time.now.strftime("%Y/%m/%d %H:%M:%S")
    @working = false
  end

  def must_stop_at
    self.class.must_stop_at(@fs_start_time || Time.now)
  end

  class << self
    def must_stop_at(fs_start_time = Time.now)
      [ $start_time + MAX_TIME, fs_start_time + MAX_FS_TIME ].min
    end
  end

  def setup_balance_cancel
    Thread.new do
      until (Time.now >= must_stop_at) || stop_condition || !working?
        sleep(30)
      end
      system("btrfs balance cancel #{@mountpoint} &>/dev/null") if working?
    end
  end

  def stop_condition
    system(@options[:stop_condition])
  end

  def working?
    @working
  end

  def balance_usage(target)
    tried(target)
    return balance_zero if target == 0

    cmd = "nice btrfs balance start -dusage=%d -musage=%d %s" %
          [ target, target, mountpoint ]
    output, status = Open3.capture2e(cmd)
    log output
    status.exitstatus
  end

  def balance_zero
    cmd =
      "nice btrfs balance start -musage=0 #{@mountpoint}"
    output, status = Open3.capture2e(cmd)
    log output
    return status.exitstatus if (Time.now > must_stop_at) || stop_condition

    cmd =
      "nice btrfs balance start -dusage=0 #{@mountpoint}"
    output, status2 = Open3.capture2e(cmd)
    log output
    [ status.exitstatus, status2.exitstatus ].max
  end

  def rebalance_needed?
    (free_wasted > free_wasted_threshold)
  end

  def free_wasted
    1 - (@unallocated.to_f / (@free * @ratio))
  end
  def used_ratio
    (@device_size - (@free * @ratio)) / @device_size
  end
  def total_device_size
    @device_size + @device_slack
  end
  def free_wasted_threshold
    return @options[:min_waste_target] if used_ratio < USED_RATIO_FOR_MIN_TARGET

    position = (used_ratio - USED_RATIO_FOR_MIN_TARGET) /
               (1 - USED_RATIO_FOR_MIN_TARGET)
    @options[:min_waste_target] +
      position * (@options[:max_waste_target] - @options[:min_waste_target])
  end

  def free_wasted_target
    free_wasted_threshold * TARGET_RATIO_FROM_THRESHOLD
  end

  # Try to guess best usage_target starting_point ?
  def start_target
    # ((1 - free_wasted_target) * 66).to_i
    10
  end
end

def change_title(msg)
  Process.setproctitle("btrfs-auto-rebalance: #{msg}")
end

def delay
  sleep_amount = (@options[:spread] * Random.rand).to_i
  return unless sleep_amount > 0

  log "delaying for #{sleep_amount}"
  change_title("waiting until #{Time.now + sleep_amount}")
  sleep sleep_amount
end

def rebalance_fs
  todo =
    filesystems.map { |fs| Btrfs.new(fs, @options) }.select(&:rebalance_needed?)
  if todo.any?
    delay
    $start_time = Time.now
    # don't use the same order on each run (one filesystem could take too long
    # and block others)
    todo.shuffle.each do |btrfs|
      change_title("balancing #{btrfs.mountpoint} until #{btrfs.must_stop_at}")
      btrfs.rebalance_if_needed
    end
  end
end

def analyse_fs
  filesystems.map do |fs|
    Btrfs.new(fs, @options).log_current_state
  end
end

if @options[:only_analyze]
  analyse_fs
else
  rebalance_fs
end
