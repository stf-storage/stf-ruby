require 'tempfile'

module STF

require "stf/context"

class Dispatcher
    EPOCH_OFFSET = 946684800
    HOST_ID_BITS = 16
    TIME_BITS    = 36
    SERIAL_BITS  = 64 - HOST_ID_BITS - TIME_BITS
    TIME_SHIFT   = HOST_ID_BITS + SERIAL_BITS
    SERIAL_SHIFT = HOST_ID_BITS
    OBJECT_ACTIVE = 1

    def initialize (ctxt, host_id = 1)
        @ctxt = ctxt
        @container = ctxt.get_container()
        if @container.nil?
            raise "No container"
        end
        @shmfile   = Tempfile.new('stf-sem')
        @parent    = $$
        @host_id   = host_id

        File.open( @shmfile, File::RDWR | File::CREAT, 0644 ) { |fh|
            fh.write( [0, 0].pack( "ql") )
        }

        ObjectSpace.define_finalizer( self, lambda { |id|
            if (@parent == $$)
                begin
                    @mutex.rm
                end
                begin
                    @shared_mem.rm
                end
            end
        })
    end

    def get(key)
        @container.get(key)
    end

    def create_id()
        id = 0

        File.open( @shmfile, File::RDWR | File::CREAT, 0644 ) { |fh|
            locked = FALSE
            lock_timeout = 5
            begin
                timeout = Time.now.to_i() + lock_timeout
                while ( timeout > Time.now.to_i() )
                    if (fh.flock( File::LOCK_NB | File::LOCK_EX ))
                        locked = TRUE;
                        break;
                    end
                    sleep rand
                end

                if (! locked)
                    raise "[Dispatcher] LOCK: Process #{$$} failed to acquire lock( tried for #{lock_timeout} seconds"
                end

                # read the currenct value...
                buf = ''
                fh.rewind()
                fh.read( 24, buf )

                host_id    = (@host_id + $$) && 0xffff # 16 bits
                time       = Time.now.to_i()
                shm        = buf.unpack( "ql" )
                shm_time   = shm[0]
                shm_serial = shm[1]
                if (shm_time == time)
                    shm_serial = shm_serial + 1
                else
                    shm_serial = 1
                end
    
                if ( shm_serial >= (1 << SERIAL_BITS) - 1 )
                    raise "Serial bits overflowed"
                end

                fh.write( [time,shm_serial].pack( "ql" ) )
    
                time_bits = (time - EPOCH_OFFSET) << TIME_SHIFT
                serial_bits = (shm_serial << SERIAL_SHIFT)
                id = time_bits | serial_bits | @host_id
            ensure
                fh.flock( File::LOCK_UN )
            end
        }

        return id
    end

    def create_object(req, bucket, object_name, input, object_size, consistency, replicas)

        object_api = get('API::Object')
        entity_api = get('API::Entity')
#        queue_api  = get('API::Queue')

        db = get('DB::Master')
        db['AutoCommit'] = FALSE

        object = nil

        begin
            old_object_id = object_api.find_object_id( bucket['id'], object_name )
            if (! old_object_id.nil?)
                $stderr.puts "[Dispatcher] Object #{object_name} on bucket #{bucket['name']} already exists (object_id = #{old_object_id})"
                object_api.mark_for_delete( old_object_id )
            end

            $stderr.puts "Looking for extension in %s" % req.path()
            suffix = File::extname( req.path() )
            if ( suffix.nil? )
                suffix = "dat"
            end

            object_id = create_id()
            internal_name = object_api.create_internal_name(suffix)

            $stderr.puts "new id = #{object_id}, internal name = #{internal_name}"

            object = object_api.create(
                object_id,
                bucket['id'],
                object_name,
                input,
                internal_name,
                object_size,
                replicas
            )

            entity_api.replicate(
                object_id,
                consistency,
                nil,
                input
            )

            db.commit

#            queue_api.enqueue( "replicate", object_id )
        rescue => e
            $stderr.puts "Error while creating object #{e}"
            $stderr.puts e.backtrace
            db.rollback
            object = nil
        ensure
            db['AutoCommit'] = TRUE
        end

        return object
    end

    def is_valid_object( req, bucket, object_name )
        object_api = @ctxt.get('API::Object')
        object_id = object_api.find_object_id( bucket['id'], object_name, OBJECT_ACTIVE )
        return ! object_id.nil?
    end
        

    def create_bucket(req, bucket_name)
        id = create_id()
        $stderr.puts "Creating bucket with ID %d" % id
        @container.get('API::Bucket').create(id, bucket_name)
    end

    def get_object(req, bucket, object_name, if_modified_since)
        object_api = @ctxt.get('API::Object')
        uri = object_api.get_any_valid_with_url(
            bucket['id'],
            object_name,
            if_modified_since
        )
        if ( uri.nil? ) 
            $stderr.puts "[Dispatcher] get_object() could not find suitable entity for %s" % object_name
            return nil;
        end

        raise STF::Dispatcher::HTTPException.new(200, { 'X-Reproxy-URL' => uri })
    end

    def get_bucket( req, bucket_name )
        bucket_api = @container.get('API::Bucket')
        bucket  = bucket_api.lookup_by_name( bucket_name )
        return bucket
    end

    def delete_object( req, bucket, object_name )
        object_api = @ctxt.get('API::Object')
        object_id  = object_api.find_object_id( bucket['id'], object_name )
        if ( object_id.nil?)
            return FALSE
        end

        begin
            object_api.mark_for_delete( object_id )
        rescue => e
            $stderr.puts "[Dispatcher] Error while marking object #{object_id} for deletion"
            return [ 500, { "Content-Type" => "text/plain" }, [ "Failed to delete object" ] ]
        end

        # queue_api.enqueue( 'delete_object', object_id )

        return TRUE
    end

    def modify_object( req, bucket, object_name, replicas )
        object_api = @ctxt.get('API::Object')
        object_id  = object_api.find_object_id( bucket['id'], object_name )
        if ( object_id.nil?)
            return FALSE
        end

        object_api.update_replica( object_id, replicas )

        # queue_api.enqueue( 'replicate', object_id )

        return TRUE
    end
end

end