module STF

class Config
    def Config.bootstrap(params)
        Config.new(params)
    end

    def initialize(params)
        @params = params
    end

    def as_hash()
        return @params
    end
end

end # end module STS
