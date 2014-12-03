local CacheEtcd = require('cache.etcd');
local cache = ifNil( CacheEtcd.new({
    prefix = '/app/cache',
    ttl = 2
}));
local key = 'key';
local val = {
    a = 'b',
    c = {
        d = 'x',
        e = {
            y = {
                'z'
            }
        }
    }
};
local ttl = 1;
local v;

ifNotNil( cache:get( key ) );
ifTrue( cache:set() );
ifTrue( cache:set( 1 ) );
ifNotTrue( cache:set( key, 1 ) );
ifTrue( cache:set( key, val, 0/0 ) );

ifNotTrue( cache:set( key, val, ttl ) );
v = ifNil( cache:get( key ) );
ifEqual( v, val );
ifNotEqual( inspect( v ), inspect( val ) );

ifNotTrue( cache:delete( key ) );

ifNotTrue( cache:set( key, val, ttl ) );
sleep( ttl + 1 );
ifNotNil( cache:get( key ) );

-- use default ttl
ifNotTrue( cache:set( key, val ) );
sleep( 3 );
ifNotNil( cache:get( key ) );
