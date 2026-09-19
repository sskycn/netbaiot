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
        loop { let before = input.len(); match framer.decode(&mut input) { Ok(Some(frame))=>{ assert!(input.len() < before); assert!(frame.len() <= 65536); }, Ok(None)=>break, Err(_)=>return } }
    }
});
