require 'sge_transform/version'
require 'csv'
require 'sequel'
require 'date'
require 'logger'
require 'toml'

module SgeTransform
  class DBMapper
    attr_reader(:db)

    def initialize(connection_string)
      @db = Sequel.connect(connection_string)
      @db.create_table? :jobs do
        primary_key :id
        String :qname
        String :hostname
        String :group
        String :owner
        String :project
        String :department
        String :jobname
        String :jobnumber
        Integer :taskid
        String :pe_taskid
        String :account
        Integer :priority
        DateTime :qsub_time
        DateTime :start_time
        DateTime :end_time
        String :granted_pe
        Integer :slots
        String :failed
        Integer :exit_status
        Integer :ru_wallclock
        Float :ru_utime
        Float :ru_stime
        Float :ru_maxrss
        Float :ru_ixrss
        Float :ru_ismrss
        Float :ru_idrss
        Float :ru_isrss
        Float :ru_minflt
        Float :ru_majflt
        Float :ru_nswap
        Float :ru_inblock
        Float :ru_oublock
        Float :ru_msgsnd
        Float :ru_msgrcv
        Float :ru_nsignals
        Float :ru_nvcsw
        Float :ru_nivcsw
        Float :cpu
        Float :mem
        Float :io
        Float :iow
        String :maxvmem
        String :arid
        DateTime :ar_submission_time
        String :category
      end
    end
  end

  class Transformer
    attr_reader(:accounting_file)
    attr_reader(:position)
    attr_reader(:db_connection_string)
    attr_reader(:flush_count)

    def initialize(accounting_file, db_connection_string, flush_count, state_file='sge_transform.state')
      @db_connection_string = db_connection_string
      @flush_count = flush_count
      @state_file = state_file
      @logger = Logger.new(STDOUT)

      if File.exist?(state_file) then
        @logger.info('State file found.')
        state = TOML.load_file(state_file, symbolize_keys: true)
        @accounting_file = state[:file]
        @position = state[:position]
      else
        @logger.info('No state file found - creating new one.')
        @accounting_file = File.expand_path(accounting_file)
        @position = 0
        update_state
      end
    end

    def update_state()
      @logger.debug("Updating statefile to position #{@position} for file #{@accounting_file}")
      state = {}
      state[:file] = @accounting_file
      state[:position] = @position
      File.open(@state_file, 'w') { |file|
        file.write(TOML.dump(state))
      }
    end

    def transform()
      headers = %w(qname hostname group owner jobname jobnumber account priority qsub_time start_time end_time failed exit_status ru_wallclock ru_utime ru_stime ru_maxrss ru_ixrss ru_ismrss ru_idrss ru_isrss ru_minflt ru_majflt ru_nswap ru_inblock ru_oublock ru_msgsnd ru_msgrcv ru_nsignals ru_nvcsw ru_nivcsw project department granted_pe slots taskid cpu mem io category iow pe_taskid maxvmem arid ar_submission_time)
      options = {
          col_sep: ':',
          headers: headers,
          skip_blanks: true,
      }

      input_file = File.open(@accounting_file, 'r')
      csv = CSV.new(input_file, options)
      csv.seek(@position)

      mapper = DBMapper.new(@db_connection_string)
      queue = []

      csv.each_with_index { |line, index|
        job = {}
        line.each { |key, value|
          if key == 'qsub_time' or key == 'start_time' or key == 'end_time' or key == 'ar_submission_time'
            job[key] = Time.at(value.to_i).utc.to_datetime
          else
            job[key] = value
          end
        }
        queue.push(job)
        if index != 0 && index % @flush_count == 0 
          mapper.db[:jobs].multi_insert(queue)
          @logger.debug("Pushed #{@flush_count} rows to database")
          @position = csv.tell
          update_state
          queue = []
        end
      }
      mapper.db[:jobs].multi_insert(queue)
      @logger.debug("Pushed #{queue.size} rows to database")
      @position = csv.tell
      update_state
      queue = []
    end
  end
end

