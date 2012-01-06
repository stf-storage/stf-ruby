require "stf/container"

module STF::Trait

module Container
    def initialize (c)
        @container = c
    end

    def get(key)
        @container.get(key)
    end
end

end
