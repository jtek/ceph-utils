#!/usr/bin/ruby

# Maximum free space wasted as percentage
MAX_WASTED_RATIO = 0.3
# Maximum unallocated space above which no check is needed
# use large value to keep data packed on the fastest cylinders if possible
UNALLOCATED_THRESHOLD_RATIO = 0.75
# Target max waste when rebalancing (not much less than max_wasted)
TARGET_WASTED_RATIO = 0.25
MAX_FAILURES = 50
MAX_REBALANCES = 100
FLAPPING_LEVEL = 3
# How fast can we move the -dusage/-musage targets
MIN_TARGET_STEP = 2
MAX_TARGET_STEP = 10
STEP_VS_WASTE = 0.75
# Maximum time allocated globally
MAX_TIME = 14400 # 4 hours
MAX_FS_TIME = 5400 # 90 minutes
SPREAD = (ARGV[0] || (24 * 3600 - MAX_TIME)).to_i # Don't overlap next run

require 'open3'

def log(msg)
  puts msg
  IO.popen("logger -p user.notice --id=$$", "w+") { |io| io.puts(msg) }
end

def filesystems
  mounts = []
  IO.popen("mount") do |io|
    io.each_line do |line|
      if match = line.match(/^(.*) on (.*) type btrfs/)
        mounts << [ match[1], match[2] ] unless mounts.map(&:first).include?(match[1])
      end
    end
  end
  return mounts.map { |m| m[1] }
end

class Btrfs
  attr_reader :mountpoint

  def initialize(mountpoint)
    @mountpoint = mountpoint
    @tried_targets = {}
    @flapping_detected = false
    refresh_usage
  end

  def tried(target)
    @tried_targets[target] ||= 0
    @tried_targets[target] += 1
    @flapping_detected = true if @tried_targets[target] == FLAPPING_LEVEL
  end

  def refresh_usage
    IO.popen("btrfs fi usage --raw '#{@mountpoint}'") do |io|
      io.each_line do |line|
        case line
	when /Device size:\s*(\d*)$/
	  @device_size = Regexp.last_match[1].to_i
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
      log_current_state
      rebalance
    end
  rescue => ex
    log "rebalance error: #{ex}\n#{ex.backtrace.join("\n")}"
  end

  def log_current_state
    log("#" * 80)
    log "%s: #{@mountpoint} rebalance started" % Time.now.strftime("%Y/%m/%d %H:%M:%S")
    log "waste %.2f%%" % (free_wasted * 100)
    log "unallocated: #{@unallocated}"
    log "ratio:       #{@ratio}"
    log "free:        #{@free}"
  end

  def rebalance
    @fs_start_time = Time.now
    cancel_reached = false
    fork_balance_cancel
    successive_failures = 0
    count = 0
    usage_target = start_target
    previous_wasted = free_wasted
    while (free_wasted > target_wasted) && (count < MAX_REBALANCES)
      if Time.now >= must_stop_at
        log "Time allocated spent, aborting"
	Process.wait @balance_cancel_pid
	log "aborted"
	cancel_reached = true
        return
      end
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
	step = [ [ ((free_wasted - target_wasted) * 100) * STEP_VS_WASTE,
	            MIN_TARGET_STEP ].max,
	         MAX_TARGET_STEP ].min.to_i
        usage_target = [ usage_target + step, 100 ].min
      end
      log("waste %.2f%%" % (free_wasted * 100))
      if @flapping_detected
        fail "flapping detected: #{FLAPPING_LEVEL} times at this usage level"
      end
    end
    if free_wasted > target_wasted
      log "#{target_wasted} not reached in #{MAX_REBALANCES} balance calls"
    end
  ensure
    kill_balance_cancel unless cancel_reached
    log "%s: #{@mountpoint} rebalance stopped" % Time.now.strftime("%Y/%m/%d %H:%M:%S")
  end

  def must_stop_at
    self.class.must_stop_at(@fs_start_time)
  end

  class << self
    def must_stop_at(fs_start_time = Time.now)
      [ $start_time + MAX_TIME, fs_start_time + MAX_FS_TIME ].min
    end
  end

  def fork_balance_cancel
    # sleep a minimum of 10 seconds to let the balance begin
    delay = [ (must_stop_at - Time.now).to_i, 10 ].max
    @balance_cancel_pid = spawn("sleep #{delay + 1} && btrfs balance cancel #{@mountpoint} &>/dev/null")
  end

  def kill_balance_cancel
    Process.kill "TERM", @balance_cancel_pid
  rescue
    # do nothing
  ensure
    # Cleanup
    Process.wait @balance_cancel_pid
  end

  def balance_usage(target)
    tried(target)
    return balance_zero if target == 0
    cmd =
      "nice btrfs balance start -dusage=#{target} -musage=#{target} #{@mountpoint}"
    output, status = Open3.capture2e(cmd)
    log output
    return status.exitstatus
  end

  def balance_zero
    cmd =
      "nice btrfs balance start -musage=0 #{@mountpoint}"
    output, status = Open3.capture2e(cmd)
    log output
    return status.exitstatus if Time.now > must_stop_at
    cmd =
      "nice btrfs balance start -dusage=0 #{@mountpoint}"
    output, status2 = Open3.capture2e(cmd)
    log output
    return [ status.exitstatus, status2.exitstatus ].max
  end

  def rebalance_needed?
    (unallocated_ratio < UNALLOCATED_THRESHOLD_RATIO) && (free_wasted > max_wasted)
  end

  def free_wasted
    1 - (@unallocated.to_f / (@free * @ratio))
  end
  def unallocated_ratio
    @unallocated.to_f / @device_size
  end
  def max_wasted
    MAX_WASTED_RATIO
  end

  def target_wasted
    TARGET_WASTED_RATIO
  end

  # Try to guess best usage_target starting_point
  def start_target
    ((1 - target_wasted) * 66).to_i
  end
end

def change_title(msg)
  Process.setproctitle("btrfs-auto-rebalance: #{msg}")
end

def delay
  sleep_amount = (SPREAD * Random.rand).to_i
  return unless sleep_amount > 0
  log "delaying for #{sleep_amount}"
  change_title("waiting until #{Time.now + sleep_amount}")
  sleep sleep_amount
end

def rebalance_fs
  todo = filesystems.map { |fs| Btrfs.new(fs) }.select(&:rebalance_needed?)
  if todo.any?
    delay
    $start_time = Time.now
    # don't use the same order on each run (one filesystem could take too long
    # and block others)
    todo.shuffle.each do |btrfs|
      change_title("balancing #{btrfs.mountpoint} until #{Btrfs.must_stop_at}")
      btrfs.rebalance_if_needed
    end
  end
end

rebalance_fs
