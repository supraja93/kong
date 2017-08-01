local _M = {}

function _M.plugin_config_iterator(dao, plugin_name)

  -- iterates over rows
  local run_rows = function(t)
    for _, row in ipairs(t) do
      coroutine.yield(row.config, function(updated_config)
        if type(updated_config) ~= "table" then
          return nil, "expected table, got " .. type(updated_config)
        end
        row.created_at = nil
        row.config = updated_config
        return dao.plugins:update(row, {id = row.id})
      end)
    end
  end

  local coro
  if dao.db_type == "cassandra" then
    coro = coroutine.create(function()
      for rows, err in dao.db.cluster:iterate([[
                SELECT * FROM plugins WHERE name = ']] .. plugin_name .. [[';
              ]]) do
        if err then
          return nil, nil, err
        end

        run_rows(rows)
      end
    end)

  elseif dao.db_type == "postgres" then
    coro = coroutine.create(function()
      local rows, err = dao.db:query([[
        SELECT * FROM plugins WHERE name = ']] .. plugin_name .. [[';
      ]])
      if err then
        return nil, nil, err
      end

      run_rows(rows)
    end)

  else
    coro = coroutine.create(function()
      return nil, nil, "unknown database type: "..tostring(dao.db_type)
    end)
  end

  return function()
    local coro_ok, config, update, err = coroutine.resume(coro)
    if not coro_ok then return false, config end  -- coroutine errored out
    if err         then return false, err    end  -- dao soft error
    if not config  then return nil           end  -- iterator done
    return true, config, update
  end
end

return _M