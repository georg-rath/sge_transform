#!/usr/bin/env ruby
require 'optparse'
require 'sge_transform'

options = {}
options[:commit_count] = 10000
opt_parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{__FILE__} [options]"

  opts.on("-aACCOUNTING_FILE", "--accounting_file=ACCOUNTING_FILE", "Path of the SGE accounting file") do |file|
    options[:accounting_file] = file
  end

  opts.on("-dDB", "--database=DB", "Connection string of the database (sequel format)") do |connection_string|
    options[:connection_string] = connection_string
  end

  opts.on("-cCOMMIT_COUNT", "--commit_count=COMMIT_COUNT", "Number of inserts to do at once") do |count|
    options[:commit_count] = count
  end
  opts.on_tail("-h", "--help", "Show this message") do
    puts opts
    exit
  end
end

opt_parser.parse!(ARGV)
raise OptionParser::MissingArgument if options[:accounting_file].nil?
raise OptionParser::MissingArgument if options[:connection_string].nil?

SgeTransform.transform(options[:accounting_file], options[:connection_string], options[:commit_count])