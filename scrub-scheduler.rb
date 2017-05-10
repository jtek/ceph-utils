#!/usr/bin/ruby

require 'time'
require 'json'
require 'pp'
require 'tempfile'

# Where is the CEPH executable?
CEPH_EXE = "ceph"
# Proportion of PG used to choose next candidate (with oldest deep-scrubs)
CHOICE_WINDOW_PROPORTION = 0.10
# TODO?: make this deep_scrub_interval - "max_expected_down_time"
# you must set the "osd deep scrub interval" to 2 weeks in ceph.conf to avoid
# the automatic deep scrubs to collide with the scheduled ones. This allows pauses
# in scrubbing for up to one week in a 2 week window, which make rebalancing events
# easier to handle
DEEP_SCRUB_TARGET_PERIOD = 7 * 24 * 3600 # 1 week
# Set "osd scrub min interval" to 2 days in ceph.conf
SCRUB_TARGET_PERIOD = 24 * 3600          # 1 day
DELAY_WHEN_UNSYNCED = 60                 # 1 minute before checking again
MAX_PARALLEL_DEEP_SCRUBS = 2
MAX_PARALLEL_SCRUBS = 4
ADMIN_MAIL = "root@localhost"
# Assume we get 1/10 of the disk rate (used to delay concurrent activity)
REPAIR_BYTE_RATE = 100_000_000 / 10

module HumanFormat
  def humanize_delay(delay)
    table = {
      "Y" => 365.25 * 24 * 3600,
      "M" => 30.5 * 24 * 3600,
      "W" => 7 * 24 * 3600,
      "D" => 24 * 3600,
      "h" => 3600,
      "m" => 60,
      "s" => 1
    }
    result = ""
    table.each { |unit,value|
      if delay > value
        count = (delay / value).floor
        result << "%02d#{unit}" % count
        delay %= value
      end
    }
    result = "0s" if result.empty?
    result
  end
end

class Pg
  ATTRIBUTES =
    [ :pgid, :version, :reported_seq, :reported_epoch, :state, :last_fresh,
    :last_change, :last_active, :last_clean, :last_became_active, :last_unstale,
    :mapping_epoch, :log_start, :ondisk_log_start, :created, :last_epoch_clean,
    :parent, :parent_split_bits, :last_scrub, :last_scrub_stamp,
    :last_deep_scrub, :last_deep_scrub_stamp, :last_clean_scrub_stamp,
    :log_size, :ondisk_log_size, :stats_invalid, :stat_sum, :stat_cat_sum, :up,
    :acting, :up_primary, :acting_primary ]
  UNSYNC_STATES_RE =
    %w(creat down replay split degraded inconsistent peering repair recover
       backfill incomplete stale remapped undersized).map do |str|
    Regexp.new(str)
  end
  attr_accessor(*ATTRIBUTES)

  def initialize(params)
    return if params.nil?
    ATTRIBUTES.each { |attr|
      self.send("#{attr}=", params[attr.to_s])
    }
    @last_scrub_stamp = Time.parse(@last_scrub_stamp) rescue nil
    @last_deep_scrub_stamp = Time.parse(@last_deep_scrub_stamp) rescue nil
    @last_clean_scrub_stamp = Time.parse(@last_clean_scrub_stamp) rescue nil
    @last_change = Time.parse(@last_change) rescue nil
    @last_active = Time.parse(@last_active) rescue nil
    @last_clean = Time.parse(@last_clean) rescue nil
    @last_fresh = Time.parse(@last_fresh) rescue nile
    @last_unstale = Time.parse(@last_unstale) rescue nile
  end

  def states
    state.split("+").map(&:downcase)
  rescue
    puts "States parsing for #{pgid} failed (#{state})"
    return []
  end

  def scrubbing?
    states.any? { |state| state.match(/scrub/) }
  end

  def deep_scrubbing?
    states.any? { |state| state.match(/deep/) }
  end

  def unsynced?
    # This list is prepared from
    # https://github.com/ceph/ceph/blob/master/doc/rados/operations/pg-states.rst
    # incomplete match are used for robustness
    UNSYNC_STATES_RE.any? do |sync_state|
      states.any? { |state| state.match(sync_state) }
    end
  end

  def repair
    puts `#{CEPH_EXE} pg repair #{pgid}`
  end

  class << self
    def load_from_ceph_pg_dump
      data = JSON.parse(`#{CEPH_EXE} pg dump_json 2>/dev/null`)
      data["pg_stats"].map { |pg_stat| Pg.new(pg_stat) }
    rescue
      puts "ERROR: can't fetch pgs"
      []
    end

    # Suppose this is done sequentially
    def repair_time(pgs)
      pgs.map { |pg|
        pg.stat_sum["num_bytes"].to_f / REPAIR_BYTE_RATE
      }.inject(0, :+) 
    end
  end
end

class ActivityState
  include HumanFormat

  def initialize
    @activities = {}
  end

  # Add new OSD to follow with their most recent activity
  def add_osdids(pgs)
    osdids = pgs.map(&:acting).flatten.uniq
    osdids.each { |osd_id|
      unless @activities[osd_id]
        # Look for most recent osd_id activity
        @activities[osd_id] = {
          deep: pgs.select { |pg|
            pg.acting.include?(osd_id) && pg.last_deep_scrub_stamp
          }.map(&:last_deep_scrub_stamp).max || Time.at(0),
          normal: pgs.select { |pg|
            pg.acting.include?(osd_id) && pg.last_scrub_stamp
          }.map(&:last_scrub_stamp).max || Time.at(0),
        }
      end
    }
  end

  def find_pg_for_scrub(pgs, scrub_type: nil)
    pgs_count = pgs.size
    osd_blacklist = []
    # Ignore scrubbing pgs and limit lookahead for best fit
    pgs = pgs.reject { |pg|
      !pg_available?(pg, osd_blacklist, scrub_type: scrub_type)
    }[0...(pgs_count * CHOICE_WINDOW_PROPORTION).to_i]
    return nil unless pgs.any?
    # Having both a pg with non-recently scrubbed OSD and the oldest deep scrub
    # aren't compatible : a compromise must be sought.
    # We start with the oldest scrub and try to find a pg with the least
    # correlation with recent OSD scrub activity.
    # In addition this won't select a pg if the first (skipped) one
    # had a common OSD as this will create situations where the oldest scrub pg
    # would be permanently blocked by new scrubs. We avoid OSD currently scrubbing
    # too.
    # The needed lookahead is expected to diminish after several passes over all
    # pgs, it is limited above and earliest scrubs have priority
    osd_blacklist |= pgs.first.acting
    period = scrub_type == :deep ? DEEP_SCRUB_TARGET_PERIOD : SCRUB_TARGET_PERIOD
    expected_interval = period.to_f / pgs.count
    step_cost = expected_interval / pgs.size
    # Find the timestamp at which we will find our candidate
    now = Time.now
    candidate_tstamp = pgs.each_with_index.map { |pg,index|
      if (osd_blacklist & pg.acting).any? && (pg != pgs.first)
        now
      else
        # artificially inflate more recent timestamps to avoid large
        # reorganizations from one pass to another
        [ pg.acting.map { |osdid| @activities[osdid][scrub_type] }.max +
          (index * step_cost), now ].min
      end
    }.min
    # Fetch the candidate
    candidate, _ = pgs.each_with_index.find { |pg,index|
      if (osd_blacklist & pg.acting).any? && (pg != pgs.first)
        false
      else
        [ pg.acting.map { |osdid| @activities[osdid][scrub_type] }.max +
          (index * step_cost), now ].min ==
          candidate_tstamp
      end
    }
    if !candidate
      puts "** no suitable candidate **"
    else
      method = "last_#{scrub_type == :deep ? "deep_" : ""}scrub_stamp"
      delay = candidate.send(method) - pgs.first.send(method)
      if delay != 0
        puts("** SKIPPED %d PGs and %s" %
             [ pgs.index(candidate), humanize_delay(delay) ])
      end
    end
    return candidate
  end

  def register_deep_scrub(pg)
    # don't favor any OSD: more predictable behaviour when choosing pg
    now = Time.now
    pg.acting.each { |osdid|
      @activities[osdid][:deep] = now
    }
  end

  def register_scrub(pg)
    # don't favor any OSD: more predictable behaviour when choosing pg
    now = Time.now
    pg.acting.each { |osdid|
      @activities[osdid][:normal] = now
    }
  end

  def format_deep_tstamps(osdids)
    osdids.map { |osdid|
      "#{osdid}: %s" % humanize_delay(Time.now - @activities[osdid][:deep])
    }.join(", ")
  end

  def format_normal_tstamps(osdids)
    osdids.map { |osdid|
      "#{osdid}: %s" % humanize_delay(Time.now - @activities[osdid][:normal])
    }.join(", ")
  end

  def pg_available?(pg, blacklist, scrub_type: nil)
    if pg.scrubbing?
      if pg.deep_scrubbing?
        if scrub_type == :deep
          puts "!! WARNING: #{pg.pgid} still deep scrubbing, skipping"
        end
        blacklist |= pg.acting
        false
      else
        if scrub_type != :deep
          puts "!! WARNING: #{pg.pgid} still scrubbing, skipping"
          blacklist |= pg.acting
          false
        else
          true
        end
      end
    else
      true
    end    
  end
end

class Scheduler
  include HumanFormat
  def initialize
    @next_deep_scrub = @next_scrub = @next_possible_repair = Time.now
    @state = ActivityState.new
  end
  
  def loop!
    loop do
      now = Time.now
      next_task = [ @next_scrub, @next_deep_scrub ].min
      if now < next_task
        pause = next_task - now
        puts "Waiting %s for next %sscrub" %
             [ humanize_delay(pause),
               @next_deep_scrub <= @next_scrub ? "deep " : "" ]
        sleep pause
      end
      pgs = Pg.load_from_ceph_pg_dump
      inconsistent_auto_repair(pgs)
      deep_scrub!(pgs) if Time.now > @next_deep_scrub
      scrub!(pgs) if Time.now > @next_scrub
    end
  end

  private

  def inconsistent_auto_repair(pgs)
    # Don't drown under repairs
    return unless Time.now > @next_possible_repair
    inconsistent_pgs = pgs.select { |pg|
      pg_states = pg.state.split("+")
      # Only deal with clean and not repairing, the other cases are left for
      # manual intervention
      pg_states.include?("inconsistent") && pg_states.include?("clean") &&
        !pg_states.include?("repair")
    }
    if inconsistent_pgs.any?
      send_inconsistent_report(inconsistent_pgs)
      inconsistent_pgs.each(&:repair)
    end
    # Leave enough time for each repair
    @next_possible_repair = Time.now + Pg.repair_time(inconsistent_pgs)
  end

  def deep_scrub!(pgs)
    if pgs.any?(&:unsynced?)
      puts "!! DELAY deep_scrub: at least one PG unsynced"
      @next_deep_scrub += DELAY_WHEN_UNSYNCED
      return
    end
    if pgs.select(&:deep_scrubbing?).size >= MAX_PARALLEL_DEEP_SCRUBS
      puts "!! DELAY deep_scrub: too many deep scrubs"
      @next_deep_scrub += 60
      return
    end
    # Fetch fresh information about PGs and order them by last_deep_scrub_stamp
    pgs = pgs.sort_by { |pg|
      pg.last_deep_scrub_stamp || Time.at(0)
    }
    time_window =
      pgs.map(&:last_deep_scrub_stamp).compact.minmax
    if time_window[0]
      puts "Deep time window: [ %s ]" %
           humanize_delay(time_window[1] - time_window[0])
    end
    total_bytes = pgs.map { |pg| pg.stat_sum["num_bytes"] || 0 }.inject(&:+)
    @state.add_osdids(pgs)

    to_scrub = @state.find_pg_for_scrub(pgs, scrub_type: :deep)
    if to_scrub
      size = to_scrub.stat_sum["num_bytes"]
      puts("%s: %s [%s], (%dMB) previous %s ago" %
           [ Time.now.to_s, to_scrub.pgid,
             @state.format_deep_tstamps(to_scrub.acting),
             size / (1024**2),
             humanize_delay(Time.now - to_scrub.last_deep_scrub_stamp) ])
      @state.register_deep_scrub(to_scrub)
      `#{CEPH_EXE} pg deep-scrub #{to_scrub.pgid} >/dev/null`
    else
      puts "ERROR: can't find pg to deep-scrub"
    end
    # Either wait according to scrubbed proportion or 1s for retrying on error
    scrubbed_proportion =
      to_scrub ? (size.to_f / total_bytes) : (1.0 / DEEP_SCRUB_TARGET_PERIOD)
    @next_deep_scrub += DEEP_SCRUB_TARGET_PERIOD * scrubbed_proportion
  end

  def scrub!(pgs)
    if pgs.any?(&:unsynced?)
      puts "!! DELAY: at least one PG unsynced"
      @next_scrub += DELAY_WHEN_UNSYNCED
      return
    end
    if pgs.select(&:scrubbing?).size >= MAX_PARALLEL_SCRUBS
      puts "!! DELAY scrub: too many scrubs"
      @next_scrub += 10
      return
    end
    # Fetch fresh information about PGs and order them by last_scrub_stamp
    pgs = pgs.sort_by { |pg|
      pg.last_scrub_stamp || Time.at(0)
    }
    time_window =
      pgs.map(&:last_scrub_stamp).compact.minmax
    if time_window[0]
      puts "Time window: [ %s ]" % humanize_delay(time_window[1] - time_window[0])
    end
    total_objects = pgs.map { |pg| pg.stat_sum["num_objects"] || 0 }.inject(&:+)
    @state.add_osdids(pgs)

    to_scrub = @state.find_pg_for_scrub(pgs, scrub_type: :normal)
    if to_scrub
      # note this is linked with objects number not size in simple scrub
      objects = to_scrub.stat_sum["num_objects"]
      puts("%s: %s [%s], (%d objects) previous %s ago" %
           [ Time.now.to_s, to_scrub.pgid,
             @state.format_normal_tstamps(to_scrub.acting),
             objects,
             humanize_delay(Time.now - to_scrub.last_scrub_stamp) ])
      @state.register_scrub(to_scrub)
      `#{CEPH_EXE} pg scrub #{to_scrub.pgid} >/dev/null`
    else
      puts "ERROR: can't find pg to scrub"
    end
    # Either wait according to scrubbed proportion or 1s for retrying on error
    scrubbed_proportion =
      to_scrub ? (objects.to_f / total_objects) : (1.0 / SCRUB_TARGET_PERIOD)
    @next_scrub += SCRUB_TARGET_PERIOD * scrubbed_proportion
  end

  def send_inconsistent_report(pgs)
    file = Tempfile.new("repair_mail")
    file.puts # empty string for CC: prompt of mail command before body
    file.puts "Found #{pgs.size} inconsistent pg(s)"
    file.puts
    pgs.each { |pg|
      file.puts "******************************"
      file.puts "#{pg.pgid}: #{pg.acting.inspect}"
      file.puts "last change: #{pg.last_change}"
      file.puts "last dscrub: #{pg.last_deep_scrub_stamp}"
      file.puts "\tdetails:"
      file.puts pg.pretty_inspect
    }
    file.close
    `cat #{file.path} | mail -s 'Repairing pg inconsistency' #{ADMIN_MAIL}`
    file.unlink
  end
end

scheduler = Scheduler.new
scheduler.loop!
