-- KEYS[1]: The inventory key (e.g., "stock:101")
-- ARGV[1]: Quantity to deduct (e.g., 1)

local stockKey = KEYS[1]
local quantityToDeduct = tonumber(ARGV[1])

-- 1. Check if the stock key exists
local currentStock = redis.call('GET', stockKey)
if not currentStock then
    return -2 -- Key does not exist
end

currentStock = tonumber(currentStock)

-- 2. Verify stock sufficiency
if currentStock >= quantityToDeduct then
    -- 3. Atomically decrement and return remaining stock
    local remaining = redis.call('DECRBY', stockKey, quantityToDeduct)
    return remaining
else
    return -1 -- Insufficient stock
end