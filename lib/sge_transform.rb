require "sge_transform/version"
require 'csv'
require 'sequel'

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
        Integer :pe_taskid
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
        Integer :ru_maxrss
        Integer :ru_ixrss
        Integer :ru_ismrss
        Integer :ru_idrss
        Integer :ru_isrss
        Integer :ru_minflt
        Integer :ru_majflt
        Integer :ru_nswap
        Integer :ru_inblock
        Integer :ru_oublock
        Integer :ru_msgsnd
        Integer :ru_msgrcv
        Integer :ru_nsignals
        Integer :ru_nvcsw
        Integer :ru_nivcsw
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

  def self.transform(accounting_file, db_connection_string, flush_count=10000)
    headers = %w(qname hostname group owner jobname jobnumber account priority qsub_time start_time end_time failed exit_status ru_wallclock ru_utime ru_stime ru_maxrss ru_ixrss ru_ismrss ru_idrss ru_isrss ru_minflt ru_majflt ru_nswap ru_inblock ru_oublock ru_msgsnd ru_msgrcv ru_nsignals ru_nvcsw ru_nivcsw project department granted_pe slots taskid cpu mem io category iow pe_taskid maxvmem arid ar_submission_time)
    options = {
        col_sep: ':',
        headers: headers,
        skip_blanks: true,
    }
    csv = CSV.new(IO.read(accounting_file), options)

    mapper = DBMapper.new(db_connection_string)
    queue = []

    csv.each_with_index { |line, index|
      job = {}
      line.each { |key, value|
        job[key] = value
      }
      queue.push(job)
      if index % flush_count == 0
        mapper.db[:jobs].multi_insert(queue)
        queue = []
      end
    }
    mapper.db[:jobs].multi_insert(queue)
  end

end

