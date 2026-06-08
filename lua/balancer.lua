-- Dynamic Load Balancer - Weighted Round Robin
-- Reads weights from shared dict and routes traffic accordingly

local weights_cache = ngx.shared.weights_cache

-- Get current weights (updated by background worker from Redis)
local aws_weight = weights_cache:get("aws") or 33
local azure_weight = weights_cache:get("azure") or 33
local do_weight = weights_cache:get("digitalocean") or 34

-- Total weight
local total_weight = aws_weight + azure_weight + do_weight

-- Ensure total is not zero
if total_weight == 0 then
    total_weight = 100
    aws_weight = 33
    azure_weight = 33
    do_weight = 34
end

-- Generate random number for weighted selection
math.randomseed(ngx.now() * 1000)
local rand = math.random(1, total_weight)

-- Select upstream based on weights using cumulative distribution
local upstream
local target_header = ngx.req.get_headers()["X-CSP-Target"]

if target_header and target_header ~= "" then
    -- Direct surge traffic to the specific target
    if target_header == "aws" or target_header == "azure" or target_header == "digitalocean" then
        upstream = target_header
    elseif target_header == "aks" then
        upstream = "azure"
    elseif target_header == "do" then
        upstream = "digitalocean"
    else
        upstream = target_header
    end
else
    if rand <= aws_weight then
        upstream = "aws"
    elseif rand <= (aws_weight + azure_weight) then
        upstream = "azure"
    else
        upstream = "digitalocean"
    end
end

-- Log the selection (for debugging)
ngx.log(ngx.DEBUG, "Selected upstream: ", upstream, 
        " (weights: aws=", aws_weight, 
        " azure=", azure_weight, 
        " do=", do_weight, 
        " rand=", rand, ")")

-- Set the upstream variable for proxy_pass
ngx.var.backend = upstream