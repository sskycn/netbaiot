#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    if data.len()>65540 { return; }
    use netbaiot_transports::tcp::TcpFramer;
    let framer=netbaiot_transports::tcp::LengthPrefixFramer{maximum:65536};
    let mut input=bytes::BytesMut::new();
    for part in data.chunks(3) {
        if input.len()+part.len()>65540 { break; }
        input.extend_from_slice(part);
        loop { match framer.decode(&mut input) { Ok(Some(_))=>{}, Ok(None)=>break, Err(_)=>return } }
    }
});
