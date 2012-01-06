require_relative './config.rb'
require_relative './container.rb'

module STF

class Context
    def initialize (home = nil)
        @home = home
        if (@home.nil?)
            @home = ENV[ 'STF_HOME'] || ENV[ 'DEPLOY_HOME' ] || Dir.pwd()
        end
    end

    def home ()
        return @home
    end

    def path_to (*list)
        return File.absolute_path(File.join(@home, *list))
    end

    # XXX private?
    def absolute_or_expand (fn)
        if ( fn == File.absolute_path(fn) )
            return fn
        else
            return path_to( fn )
        end
    end

    # XXX private?
    def set_container (c)
        @container = c
    end

    def get_container ()
        return @container
    end

    def get(name)
        return @container.get(name)
    end

    def Context.bootstrap(x_config_fn = nil, x_container_fn = nil)
        object = new()

        if ( ! x_config_fn.nil? )
            config_fn = object.absolute_or_path_to( x_config_fn )
        elsif ( ! ENV[ "STF_CONFIG" ].nil? )
            config_fn = object.absolute_or_path_to( ENV[ "STF_CONFIG" ] )
        else
            config_fn = object.path_to( "etc", "config.rb" )
        end

        if ( ! x_container_fn.nil? )
            container_fn = object.absolute_or_path_to( x_container_fn )
        elsif ( ! ENV[ "STF_CONFIG" ].nil? )
            container_fn = object.absolute_or_path_to( ENV[ "STF_CONFIG" ] )
        else
            container_fn = object.path_to( "etc", "container.rb" )
        end

        load config_fn
        load container_fn
        object.set_container( STF::Container.instance() )

        return object
    end
end


end # end module STF