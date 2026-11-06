"""Independent interop oracle. Requires nats-py==2.16.0; test-only."""
import asyncio
import nats
async def main():
    nc=await nats.connect('nats://127.0.0.1:14222')
    try:
        js=nc.jetstream()
        kv=await js.key_value('MN_ORACLE')
        assert (await kv.get('moon.value')).value == b'from-moonbit'
        await kv.put('python.value',b'from-python')
        objects=await js.object_store('MN_ORACLE')
        assert (await objects.get('moon.bin')).data == bytes(i%251 for i in range(300000))
        assert (await objects.get('moon-empty')).data == b''
        await objects.put('python.bin',bytes(i%199 for i in range(200000)))
        await objects.put('python-empty',b'')
        print('oracle: nats-py read MoonNATS KV and Object Store PASS')
    finally:
        await nc.close()
asyncio.run(main())
