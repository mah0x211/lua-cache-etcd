--[[

  Copyright (C) 2014 Masatoshi Teruya

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:
 
  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.
 
  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
 
  etcd.lua
  lua-cache-etcd
  
  Created by Masatoshi Teruya on 14/12/03.
  
--]]

-- modules
local typeof = require('util.typeof');
local Cache = require('cache');
local Etcd = require('etcd.luasocket');

-- private
local function request( cli, method, failval, ... )
    local res, err = cli[method]( cli, ... );
    
    if err then
        return failval, err;
    -- success
    elseif res.status == 200 or res.status == 201 then
        return failval == false or res.body.node.value;
    -- timeout
    elseif res.status == 408 then
        return failval, '408 request timed out';
    end
    
    return failval, ('%d %s'):format( res.body.errorCode, res.body.message );
end

-- class
local CacheEtcd = require('halo').class.CacheEtcd;


function CacheEtcd:init( opts )
    local ttl, cli, err;
    
    if typeof.table( opts ) and typeof.finite( opts.ttl ) then
        ttl = opts.ttl > 0 and opts.ttl or 0;
    end
    
    cli, err = Etcd.new( opts );
    if err then
        return nil, err;
    end
    protected(self).cli = cli;
    
    return Cache.new( self, ttl );
end


function CacheEtcd:set( key, val, ttl )
    return request( protected(self).cli, 'set', false, key, val, ttl );
end


function CacheEtcd:get( key )
    return request( protected(self).cli, 'get', nil, key );
end


function CacheEtcd:delete( key )
    return request( protected(self).cli, 'delete', false, key );
end


return CacheEtcd.exports;
