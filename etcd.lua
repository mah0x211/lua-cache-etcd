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
local toStatusLineName = require('httpconsts.status').toStatusLineName;
local Cache = require('cache');

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
    elseif typeof.table( res.body ) then
        return failval, ('%d %s'):format( res.body.errorCode, res.body.message );
    end
    
    return failval, ('%d %s'):format( 
        res.status, 
        toStatusLineName( res.status ) or '' 
    );
end

-- class
local Etcd = require('halo').class.Etcd;

Etcd:property {
    protected = {
        cli = require('etcd.luasocket')
    }
};

function Etcd:init( opts )
    local own = protected(self);
    local ttl = typeof.table( opts ) and opts.ttl or nil;
    local cli, err;
    
    own.cli, err = own.cli.new( opts );
    if err then
        return nil, err;
    end
    
    return Cache.new( self, ttl );
end


function Etcd:set( key, val, ttl )
    return request( protected(self).cli, 'set', false, key, val, ttl );
end


function Etcd:get( key )
    return request( protected(self).cli, 'get', nil, key );
end


function Etcd:delete( key )
    return request( protected(self).cli, 'delete', false, key );
end


return Etcd.exports;
