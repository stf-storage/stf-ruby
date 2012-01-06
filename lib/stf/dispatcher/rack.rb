require 'rack/request'
require 'rack/response'

module STF

class Dispatcher

class HTTPException < RuntimeError
    def initialize (code, hdrs = {}, content = [])
        @code = code
        @headers = hdrs
        @content = content

        # XXX need to lower case all keys?
        if ( @code.to_s !~ /^1\d\d|^[23]04$/ && hdrs[ "Content-Type" ].nil? )
            hdrs[ "Content-Type" ] = "text/plain"
        end

        super("Status: %s" % @code)
    end

    def as_response ()
        return [ @code, @headers, @content ]
    end
end # End STF::HTTPException

# Defines the interface of STF::Dispatcher
class Rack
    DEBUG                     = ENV[ "STF_DEBUG" ]
    REPLICATION_HEADER        = 'X_STF_REPLICATION_COUNT'
    RECURSIVE_DELETE_HEADER   = 'X_STF_RECURSIVE_DELETE'
    CONSISTENCY_HEADER        = 'X_STF_CONSISTENCY'
    DEFAULT_REPLICATION_COUNT = 2

    def initialize (impl)
        @impl = impl
    end

    def call(env) 
        req = ::Rack::Request.new( env )

        case req.request_method()
        when 'GET', 'HEAD' then
            res = get_object( req )
        when 'PUT' then
            cl = req.content_length()
            if ( cl.nil? || cl == 0 )
                res = create_bucket( req )
            else
                res = create_object( req )
            end
        when 'DELETE' then
            res = delete_object( req )
        when 'POST' then
            res = modify_object( req )
        else 
            res = [ 400, { "Content-Type" => "text/plain" }, [] ]
        end

        require 'pp'
        PP.pp(res)

        return res

        rescue STF::Dispatcher::HTTPException => e
            require 'pp'
            PP.pp(e.as_response())
            return e.as_response()
    end

    def parse_names(req)
        result = req.path.match(/^\/([^\/]+)(?:\/(.+)$)?/)
        if (result.nil?)
            if DEBUG
               $stderr.puts "Could not parse bucket/object name from '%s'" % req.path
            end
            return nil
        end
        return [ result[1], result[2] ]
    end

    def create_object(req)
        require 'pp'
        PP.pp(req)

        result = parse_names(req)
        if (result.nil?)
            return [ 400, { "Content-Type" => "text/plain" }, [] ]
        end

        bucket_name = result[0]
        bucket = @impl.get_bucket( req, bucket_name )

        if (bucket.nil?)
            return [ 500, { "Content-Type" => "text/plain" }, [ "Failed to find bucket" ] ]
        end

        object_name = result[1]
        if (object_name.nil?)
            return [ 400, { "Content-Type" => "text/plain" }, [ "Could not extract object name" ] ]
        end

        object = @impl.create_object(
            req,
            bucket,
            object_name,
            req.env['rack.input'],
            req.content_length(),
            req.env[ CONSISTENCY_HEADER ] || 0,
            req.env[ REPLICATION_HEADER ] ||
                DEFAULT_REPLICATION_COUNT
        )
        if (object.nil?)
            return [ 500, { "Content-Type" => "text/plain" }, [ "Failed to create object"] ]
        end
        return [ 200, { "Content-Type" => "text/plain" }, [ "Created" ] ]
    end

    def create_bucket(req)
        result = parse_names(req)
        if (result.nil?)
            return [ 400, { "Content-Type" => "text/plain" }, [] ]
        end

        bucket_name = result[0]
        bucket = @impl.get_bucket( req, bucket_name )

        if (! bucket.nil?)
            return [ 204, { "Content-Type" => "text/plain" }, [] ]
        end

        bucket = @impl.create_bucket( req, bucket_name )
        if ( bucket.nil? )
            return [ 500, { "Content-Type" => "text/plain" }, [ "Failed to create bucket" ] ]
        end

        return [ 201, { "Content-Type" => "text/plain" }, [ "Created bucket"] ]
    end

    def get_object(req)
        result = parse_names(req)
        if (result.nil?)
            return [ 400, { "Content-Type" => "text/plain" }, [] ]
        end

        bucket_name = result[0]
        bucket = @impl.get_bucket( req, bucket_name )

        if (bucket.nil?)
            return [ 500, { "Content-Type" => "text/plain" }, [ "Failed to find bucket" ] ]
        end

        if_modified_since = req.env[ 'IF_MODIFIED_SINCE' ]

        object = @impl.get_object( req, bucket, result[1], if_modified_since )

        if ( object.nil? ) 
            return [ 404, { "Content-Type" => "text/plain" }, [ "Failed to get object '%s'" % req.path ] ];
        end

        return [ 200,
            { "Content-Type" => "application/octet-stream" },
            [ req.head? ? '' : object.content ]
        ]
    end

    def delete_object (req)
        result = parse_names(req)
        if (result.nil?)
            return [ 400, { "Content-Type" => "text/plain" }, [] ]
        end

        bucket_name = result[0]
        object_name = result[1]
        bucket = @impl.get_bucket( req, bucket_name )

        if (bucket.nil?)
            if ( object_name.nil? )
                return [ 404, { "Content-Type" => "text/plain" }, [ "Failed to find bucket" ] ]
            else
                return [ 500, { "Content-Type" => "text/plain" }, [ "Failed to find bucket" ] ]
            end
        end

        if ( object_name.nil? )
            rv = @impl.delete_bucket(
                req,
                bucket,
            )
            if (! rv)
                return [ 500, { "Content-Type" => "text/plain" }, [ "Failed to delete bucket" ] ]
            end

            return [ 204, {}, [] ]
        end

        if ( ! @impl.is_valid_object( req, bucket, object_name ) )
            return [ 404, { "Content-Type" => "text/plain" }, [ "No such object" ] ]
        end

        if ( @impl.delete_object( req, bucket, object_name) )
            return [ 204, {}, [] ];
        else
            return [ 500, { "Content-Type" => "text/plain" }, [ "Failed to delete object" ] ]
        end
    end

    def modify_object (req)
        result = parse_names(req)
        if (result.nil?)
            return [ 400, { "Content-Type" => "text/plain" }, [] ]
        end

        bucket_name = result[0]
        object_name = result[1]
        bucket = @impl.get_bucket( req, bucket_name )

        if (bucket.nil?)
            return [ 404, { "Content-Type" => "text/plain" }, [ "Failed to find bucket" ] ]
        end

        if ( ! @impl.is_valid_object( req, bucket, object_name ) )
            return [ 404, { "Content-Type" => "text/plain" }, [ "No such object" ] ]
        end

        replicas = req.env[ REPLICATION_HEADER ] || DEFAULT_REPLICATION_COUNT
        if ( @impl.modify_object( req, bucket, object_name, replicas ) )
            return [ 204, {}, [] ]
        else
            return [ 500, { "Content-Type" => "text/plain" }, [ "Failed to modify object" ] ]
        end
    end

end # end class Dispatcher::Rack

end

end
