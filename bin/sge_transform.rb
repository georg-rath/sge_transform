#!/usr/bin/env ruby
require 'optparse'
require 'sge_transform'

options = {}
options[:commit_count] = 10000
opt_parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{__FILE__} [options]"

  opts.on("-a ACCOUNTING_FILE", "--accounting_file=ACCOUNTING_FILE", "Path of the SGE accounting file") do |file|
    options[:accounting_file] = file
  end

  opts.on("-d DB", "--database=DB", "Connection string of the database (sequel format)") do |connection_string|
    options[:connection_string] = connection_string
  end

  opts.on("-c COMMIT_COUNT", "--commit_count=COMMIT_COUNT", "Number of inserts to do at once") do |count|
    options[:commit_count] = count
  end
  opts.on_tail("-h", "--help", "Show this message") do
    puts opts
    exit
  end
end

begin
  opt_parser.parse!
  mandatory = [:accounting_file, :connection_string]                                         # Enforce the presence of
  missing = mandatory.select{ |param| options[param].nil? }        # the -t and -f switches
  unless missing.empty?                                            #
    puts "Missing options: #{missing.join(', ')}"                  #
    puts opt_parser                                                  #
    exit                                                           #
  end                                                              #
rescue OptionParser::InvalidOption, OptionParser::MissingArgument      #
  puts $!.to_s                                                           # Friendly output when parsing fails
  puts opt_parser                                                      #
  exit                                                                   #
end

SgeTransform.transform(options[:accounting_file], options[:connection_string], options[:commit_count])
