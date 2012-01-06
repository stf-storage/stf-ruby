$LOAD_PATH.push( File.join( Dir.pwd, "lib" ) )

require "stf/dispatcher/rack.rb"
require "stf.rb"

ctxt = STF::Context.bootstrap()
impl = STF::Dispatcher.new( ctxt )
run STF::Dispatcher::Rack.new( impl )