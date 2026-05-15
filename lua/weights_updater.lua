local weights_updater = {}

function weights_updater.update()
    ngx.req.read_body()
    local data = ngx.req.get_body_data()

    if not data then
        ngx.status = 400
        ngx.say("No data provided")
        return
    end

    local json = require "cjson"
    local ok, weights_data = pcall(json.decode, data)
    if not ok then
        ngx.status = 400
        ngx.say("Invalid JSON")
        return
    end

    local weights = ngx.shared.weights_cache

    if weights_data.aws_wi then
        weights:set("aws_wi", tonumber(weights_data.aws_wi))
    end
    if weights_data.aks_wi then
        weights:set("aks_wi", tonumber(weights_data.aks_wi))
    end
    if weights_data.do_wi then
        weights:set("do_wi", tonumber(weights_data.do_wi))
    end

    ngx.say("Weights updated successfully")
end

return weights_updater