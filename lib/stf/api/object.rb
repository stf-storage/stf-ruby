require 'httpclient'
require 'securerandom'
require "stf/trait/container"

module STF::API

class Object 
    OBJECT_ACTIVE = 1
    STORAGE_MODE_READ_ONLY = 0
    STORAGE_MODE_READ_WRITE = 1

    include STF::Trait::Container

    def create (object_id, bucket_id, object_name, input, internal_name, size, replicas)
        db = get('DB::Master')
        db.execute(<<EOSQL, object_id, bucket_id, object_name, internal_name, size, replicas)
            INSERT INTO object (id, bucket_id, name, internal_name, size, num_replica, created_at) VALUES (?, ?, ?, ?, ?, ?, UNIX_TIMESTAMP(NOW()))
EOSQL
    end

    def find (id)
        db = get('DB::Master')
        st = db.execute( "SELECT * FROM object WHERE id = ?", id )
        obj = st.fetch_hash()
        st.finish()
        return obj
    end

    def find_object_id( bucket_id, object_name, status = nil)
        db = get('DB::Master')
        bind = [ bucket_id, object_name ]
        sql = "SELECT id FROM object WHERE bucket_id = ? AND name = ?"
        if (! status.nil?)
            bind.push( status )
            sql += " AND status = ?"
        end
        $stderr.puts "find_object_id: sql = #{sql}, bind = #{bind}"
        row = db.select_one( sql, *bind )
        return row.nil? ? nil : row[0]
    end

    def get_any_valid_with_url( bucket_id, object_name, if_modified_since = nil )
        object_id = find_object_id( bucket_id, object_name, OBJECT_ACTIVE )
        if ( object_id.nil? )
#                printf STDERR "[Get Entity] Could not get object_id from bucket ID (%s) and object name (%s)\n",
#                $bucket_id,
#                $object_name
#            ;
            return
        end

        # cache get
        entities = nil
        if ( entities.nil? )
            db = get('DB::Master')
            sth = db.execute(<<EOSQL, object_id, STORAGE_MODE_READ_ONLY, STORAGE_MODE_READ_WRITE)
                SELECT s.uri, o.internal_name
                FROM object o JOIN entity e ON o.id = e.object_id
                              JOIN storage s ON s.id = e.storage_id 
                WHERE
                    o.id = ? AND
                    o.status = 1 AND 
                    e.status = 1 AND
                    s.mode IN ( ?, ? )
EOSQL
            entities = []
            sth.each do |row|
                entities.push(row.join("/"))
            end
            sth.finish
        end

        entities.sort! { |a, b|
            a.hash <=> b.hash
        }

        if ( ! if_modified_since.nil? )
            headers = { "If-Modified-Since" => $if_modified_since }
        end

        # cache set
        client = HTTPClient.new
        repair = 0
        fastest = nil
        entities.each { |entity| 
            res = client.head( entity, nil, headers )
            if (HTTP::Status.successful?(res.status) ) 
                fastest = entity
                # found it, bail out
                break
            elsif (res.status == 304)
                # not modified, bail out
                raise STF::Dispatcher::Exception.new( 304 )
            else 
                $stderr.puts "[Get Entity] + HEAD #{entity} failed: #{res.status}"
                repair = repair + 1
            end
        }

        if (repair > 0)
            # send to repair queue
        end

        return fastest
    end

    def create_internal_name (suffix = "dat")
        chars = ('a'..'z').to_a
        picks = []
        for i in 1..30
            picks.push( chars[SecureRandom.random_number(26)] )
        end

        fname = picks.join("") + ".#{suffix}"
        return File::join(picks[0], picks[1], picks[2], picks[3], fname)
    end

    def mark_for_delete( object_id )
        db = get('DB::Master')
        db.do( <<EOSQL, object_id + 1)
            REPLACE INTO deleted_object SELECT * FROM object WHERE id = ?
EOSQL
        db.do( <<EOSQL, object_id )
            DELETE FROM object WHERE id = ?
EOSQL
        return TRUE
    end

    def update_replica( object_id, replicas )
        db = get('DB::Master')
        db.do( <<EOSQL, replicas, object_id )
            UPDATE object SET num_replica = ? WHERE id = ?
EOSQL
    end
end # class Object

end # module STF::API
