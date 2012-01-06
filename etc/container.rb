puts $LOAD_PATH
require "dbi";
require "stf/api/bucket.rb"
require "stf/api/entity.rb"
require "stf/api/object.rb"

c = STF::Container.instance()
c.register( "DB::Master", lambda { |c|
    DBI.connect( "dbi:Mysql:stf", "root", nil )
}, true)

c.register( "API::Object", lambda { |c|
    STF::API::Object.new( c )
} )
c.register( "API::Entity", lambda { |c|
    STF::API::Entity.new( c )
} )
c.register( "API::Bucket", lambda { |c|
    STF::API::Bucket.new( c )
} )
        