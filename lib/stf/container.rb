module STF

class Container
    @@instance = nil

    def Container.instance()
        if (@@instance.nil?)
            @@instance = STF::Container.new();
        end
        return @@instance
    end

    def Container.register(stuff, scoped)
        stuff.each_pair { |key, value|
            STF::Container.instance().register(key, value, scoped)
        }
    end

    def initialize ()
        @scoped_registry = {}
        @scoped_objects = {}
        @registry = {}
        @objects = {}
    end

    def register (key, value, scoped = false)
        puts "register #{key}"
        if (value.class <= Proc)
            if (scoped) 
                @scoped_registry[ key ] = value
            elsif
                @registry[ key ] = value
            end
        else
            @objects[ key ] = value
        end
    end

    def get(key)
        puts @scoped_registry
        is_scoped = @scoped_registry.has_key?( key )
        if ( is_scoped ) 
            object = @scoped_objects[key]
        else
            object = @objects[key]
        end

        if ( object.nil? )
            if ( is_scoped )
                p = @scoped_registry[key]
                object = p.call( self )
                if ( ! object.nil? )
                    @scoped_objects[key] = object
                end
            else
                p = @registry[key]
                if ( ! p.nil? )
                    object = p.call( self )
                    if ( ! object.nil?)
                        @objects[key] = object
                    end
                end
            end

            if (object.nil?)
                raise RuntimeError, "#{key} could not be found in container"
            end
        end

        return object
    end
end

end 