require 'stf/trait/container.rb'
require 'httpclient'

module STF::API

class Entity
    STORAGE_MODE_READ_ONLY = 0
    STORAGE_MODE_READ_WRITE = 1
    ENTITY_ACTIVE = 1

    include STF::Trait::Container

    def replicate(object_id, replicas, content, input)
        db = get('DB::Master')
        client = HTTPClient.new
        object_api = get('API::Object')

        object = object_api.find(object_id)
        if ( object.nil?)
            return 0
        end

        if ( replicas.nil? )
            row = db.select_one( <<EOSQL, object_id, STORAGE_MODE_READ_WRITE )
                SELECT count(*)
                    FROM entity e JOIN storage s ON e.storage_id = s.id
                    WHERE object_id = ? AND s.mode = ?
EOSQL
            count = 0
            if (! row.nil?)
                count = row[0]
            end

            if (! @max_num_replica.nil? && @max_num_replica <= count)
                return 0
            end

            if (object['num_replica'] <= count)
                return 0
            end
            replicas = object['num_replica'] - count
        end

        if ( replicas <= 0 )
            replicas = 1
        end

        if ( content.nil? )
            if ( ! input.nil? )
                # read from input
                size  = object['size'].to_i 
                sofar = 0
                content = ''
                while ( sofar < size )
                    buf = input.read( size )
                    sofar += buf.size
                    content += buf
                end
            elsif ( ! object.nil? )
                # fetch content from existing object
                storages = db.select_all( <<EOSQL, STORAGE_MODE_READ_ONLY, STORAGE_MORE_READ_WRITE, object_id )
                    SELECT s.id, s.uri
                        FROM storage s JOIN entity e ON s.id = e.storage_id
                        WHERE s.mode IN (?, ?) AND e.object_id = ?
EOSQL
                if ( storages.size <= 0 )
                    return 0
                end

                for storage in storages
                    uri = [ storage['uri'], object['internal_name'] ].join("/")
                    res = client.get( uri )
                    if ( ! HTTP::Status.successful?(res.status) )
                        next
                    end

                    content = res.body
                    break
                end
            end
        end

        if ( content.nil? )
            return 0
        end

        storages = db.select_all( <<EOSQL, STORAGE_MODE_READ_WRITE, object_id )
            SELECT s.id, s.uri
                FROM storage s 
                WHERE s.mode = ? AND s.id NOT IN (
                    SELECT storage_id FROM entity WHERE object_id = ?
                )
EOSQL
        if ( storages.size < replicas )
            $stderr.puts "[ Replicate] Wanted #{replicas} storages, but found #{storages.size}\n"

            if ( storages.size < 1 )
                $stderr.puts "[ Replicate] In fact, no storages were available. Bailing out of replicate()\n"
                return 0
            end
        end

        headers = {
            'Content-Length' => object['size'],
            'X-STF-Object-Timestamp' => object['created_at']
        }

        sth = db.prepare( <<EOSQL )
            INSERT
                INTO entity (object_id, storage_id, status, created_at)
                VALUES (?, ?, ?, UNIX_TIMESTAMP(NOW()))
EOSQL
        ok_count = 0
        for storage in storages
            uri = [ storage['uri'], object['internal_name'] ].join("/")
            res = client.put(uri, content, headers)

            if ( HTTP::Status.successful?( res.status ) )
                ok_count = ok_count + 1
                sth.execute( object_id, storage['id'], ENTITY_ACTIVE )
            else
                $stderr.puts "[ Replicate] Request to replicate to $uri failed"
            end
        end

        if (ok_count <= 0)
            raise "*** ALL REQUESTS TO REPLICATE FAILED (wanted #{replicas})"
        end

        return ok_count
    end
end

end
