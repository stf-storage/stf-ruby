require 'stf/trait/container'

module STF::API

class Bucket
    include STF::Trait::Container

    def lookup_by_name (name)
        db = get('DB::Master')
        st = db.execute( "SELECT * FROM bucket WHERE name = ?", name )
        obj = st.fetch_hash()
        st.finish()
        return obj
    end

    def create(id, name)
        db = get('DB::Master')
        db.execute( <<EOSQL, id, name )
            INSERT INTO bucket (id, name, created_at) VALUES (?, ?, UNIX_TIMESTAMP(NOW()))
EOSQL
        return true
    end

end

end
