//! Allocation-free structural preflight. No length from the wire is used for allocation.
//! At most 64 stack levels and fields+8 map pairs; every node/string is charged
//! conservatively for the subsequent typed serde decode (including map nodes).
use netbaiot_core::{CodecError, CodecLimits};
#[derive(Clone, Copy)]
pub(crate) enum Format {
    Cbor,
    Msgpack,
}
pub(crate) struct Budget<'a> {
    pub limits: &'a CodecLimits,
    used: usize,
}
impl<'a> Budget<'a> {
    pub fn new(limits: &'a CodecLimits, input: &[u8]) -> Result<Self, CodecError> {
        if input.len() > limits.input_bytes
            || input.len() > limits.decoded_bytes
            || limits.output_messages == 0
        {
            return Err(CodecError);
        }
        Ok(Self { limits, used: 0 })
    }
    pub fn charge(&mut self, n: usize) -> Result<(), CodecError> {
        self.used = self.used.checked_add(n).ok_or(CodecError)?;
        if self.used > self.limits.decoded_bytes {
            return Err(CodecError);
        }
        Ok(())
    }
}
pub(crate) struct Reader<'a> {
    pub bytes: &'a [u8],
    pub at: usize,
}
impl<'a> Reader<'a> {
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let end = self.at.checked_add(n).ok_or(CodecError)?;
        let v = self.bytes.get(self.at..end).ok_or(CodecError)?;
        self.at = end;
        Ok(v)
    }
    pub fn byte(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }
    pub fn uint(&mut self, n: usize) -> Result<u64, CodecError> {
        let mut v = 0;
        for b in self.take(n)? {
            v = (v << 8) | u64::from(*b);
        }
        Ok(v)
    }
    pub fn varint(&mut self) -> Result<u64, CodecError> {
        let mut n = 0;
        for i in 0..10 {
            let b = self.byte()?;
            if i == 9 && b > 1 {
                return Err(CodecError);
            }
            n |= u64::from(b & 127) << (i * 7);
            if b & 128 == 0 {
                return Ok(n);
            }
        }
        Err(CodecError)
    }
    pub fn len(v: u64) -> Result<usize, CodecError> {
        usize::try_from(v).map_err(|_| CodecError)
    }
}
struct Scan<'a, 'l> {
    reader: Reader<'a>,
    budget: Budget<'l>,
    pairs: usize,
    format: Format,
}
impl Scan<'_, '_> {
    fn text(&mut self, n: u64) -> Result<(), CodecError> {
        let n = Reader::len(n)?;
        if n > self.budget.limits.field_bytes.max(64) {
            return Err(CodecError);
        }
        let b = self.reader.take(n)?;
        std::str::from_utf8(b).map_err(|_| CodecError)?;
        self.budget.charge(n.checked_mul(3).ok_or(CodecError)?)
    }
    fn map(&mut self, n: u64, depth: usize) -> Result<(), CodecError> {
        if depth > self.budget.limits.nesting_depth.min(64) {
            return Err(CodecError);
        }
        let n = Reader::len(n)?;
        self.pairs = self.pairs.checked_add(n).ok_or(CodecError)?;
        if self.pairs > self.budget.limits.fields.saturating_add(8)
            || n > (self.reader.bytes.len() - self.reader.at) / 2
        {
            return Err(CodecError);
        }
        for _ in 0..n {
            self.value(depth + 1, true)?;
            self.value(depth + 1, false)?;
        }
        Ok(())
    }
    fn cbor_arg(&mut self, info: u8) -> Result<u64, CodecError> {
        match info {
            0..=23 => Ok(u64::from(info)),
            24 => self.reader.uint(1),
            25 => self.reader.uint(2),
            26 => self.reader.uint(4),
            27 => self.reader.uint(8),
            _ => Err(CodecError),
        }
    }
    fn value(&mut self, depth: usize, key: bool) -> Result<(), CodecError> {
        // Only containers consume nesting depth; scalar leaves do not.
        self.budget.charge(128)?;
        let b = self.reader.byte()?;
        match self.format {
            Format::Cbor => {
                let major = b >> 5;
                let info = b & 31;
                if key && major != 3 {
                    return Err(CodecError);
                }
                match major {
                    0 | 1 => {
                        self.cbor_arg(info)?;
                        Ok(())
                    }
                    3 => {
                        let n = self.cbor_arg(info)?;
                        self.text(n)
                    }
                    5 => {
                        let n = self.cbor_arg(info)?;
                        self.map(n, depth)
                    }
                    7 => match info {
                        20..=22 => Ok(()),
                        25 => {
                            self.reader.take(2)?;
                            Ok(())
                        }
                        26 => {
                            self.reader.take(4)?;
                            Ok(())
                        }
                        27 => {
                            self.reader.take(8)?;
                            Ok(())
                        }
                        _ => Err(CodecError),
                    },
                    _ => Err(CodecError),
                }
            }
            Format::Msgpack => {
                if key && !matches!(b,0xa0..=0xbf|0xd9..=0xdb) {
                    return Err(CodecError);
                }
                match b {
                    0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => Ok(()),
                    0xa0..=0xbf => self.text(u64::from(b & 31)),
                    0x80..=0x8f => self.map(u64::from(b & 15), depth),
                    0xd9..=0xdb => {
                        let n = self.reader.uint(1 << (b - 0xd9))?;
                        self.text(n)
                    }
                    0xde | 0xdf => {
                        let n = self.reader.uint(if b == 0xde { 2 } else { 4 })?;
                        self.map(n, depth)
                    }
                    0xcc | 0xd0 => {
                        self.reader.take(1)?;
                        Ok(())
                    }
                    0xcd | 0xd1 => {
                        self.reader.take(2)?;
                        Ok(())
                    }
                    0xca | 0xce | 0xd2 => {
                        self.reader.take(4)?;
                        Ok(())
                    }
                    0xcb | 0xcf | 0xd3 => {
                        self.reader.take(8)?;
                        Ok(())
                    }
                    _ => Err(CodecError),
                }
            }
        }
    }
}
pub(crate) fn preflight(input: &[u8], l: &CodecLimits, format: Format) -> Result<(), CodecError> {
    let mut s = Scan {
        reader: Reader {
            bytes: input,
            at: 0,
        },
        budget: Budget::new(l, input)?,
        pairs: 0,
        format,
    };
    // Container depth counts the outer map as 1, matching JSON's convention.
    s.value(1, false)?;
    if s.reader.at != input.len() {
        return Err(CodecError);
    }
    Ok(())
}
