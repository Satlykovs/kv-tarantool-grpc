box.cfg {
    listen = 3301,
    memtx_memory = 2 * 1024 * 1024 * 1024,
    net_msg_max = 4096,
    readahead = 16384
}

box.once("init", function()
    local kv = box.schema.space.create('KV', { if_not_exists = true })

    kv:format({
        { name = 'key',   type = 'string' },
        { name = 'value', type = 'varbinary', is_nullable = true }
    })

    kv:create_index('primary', {
        type = 'tree',
        parts = { 'key' },
        if_not_exists = true
    })


    box.schema.user.grant('guest', 'read,write,execute', 'universe', nil, { if_not_exists = true })
end)
