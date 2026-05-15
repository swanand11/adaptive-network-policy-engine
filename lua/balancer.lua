local balancer = {}

function balancer.balance()
    local weights = ngx.shared.weights_cache

    local aws_wi = weights:get("aws_wi") or 33
    local aks_wi = weights:get("aks_wi") or 33
    local do_wi = weights:get("do_wi") or 34

    local total = aws_wi + aks_wi + do_wi
    if total == 0 then
        total = 100
        aws_wi = 33
        aks_wi = 33
        do_wi = 34
    end

    -- Simple weighted random selection
    local rand = math.random(1, total)
    local cumulative = 0

    cumulative = cumulative + aws_wi
    if rand <= cumulative then
        ngx.var.backend = "aws_backend"
        return
    end

    cumulative = cumulative + aks_wi
    if rand <= cumulative then
        ngx.var.backend = "aks_backend"
        return
    end

    ngx.var.backend = "do_backend"
end

return balancer