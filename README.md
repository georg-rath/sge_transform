# SgeTransform

Transform a Sun GridEngine accounting file from a flat file to an RDBMS.
Database output is done via the Sequel library. PostgreSQL and MySQL adapters are installed as part of this gem.
If you need other adapters you need to install them on your own.

Data is pumped into the database as a single table with the following fields:
    qname hostname group owner jobname jobnumber account priority qsub_time start_time end_time failed exit_status
    ru_wallclock ru_utime ru_stime ru_maxrss ru_ixrss ru_ismrss ru_idrss ru_isrss ru_minflt ru_majflt ru_nswap
    ru_inblock ru_oublock ru_msgsnd ru_msgrcv ru_nsignals ru_nvcsw ru_nivcsw project department granted_pe slots
    taskid cpu mem io category iow pe_taskid maxvmem arid ar_submission_time

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'sge_transform'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install sge_transform

## Usage

./bin/sge_transform -h
