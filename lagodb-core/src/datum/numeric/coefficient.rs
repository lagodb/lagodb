//! Unsigned 256-bit coefficients of the scaled NUMERIC domains.

const DECIMAL_DIGITS_PER_GROUP: i32 = 4;
const NUMERIC_BASE: u64 = 10_000;
pub(super) const I256_BYTES: usize = 32;
const I256_LIMBS: usize = 4;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Unsigned256 {
    /// Little-endian limbs, so division walks this array in reverse.
    limbs: [u64; I256_LIMBS],
}

impl Ord for Unsigned256 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.limbs.iter().rev().cmp(other.limbs.iter().rev())
    }
}

impl PartialOrd for Unsigned256 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Unsigned256 {
    pub(super) const ZERO: Self = Self {
        limbs: [0; I256_LIMBS],
    };

    /// `10^exponent` for an exponent of at most 77, the largest decimal power
    /// below 2^256.
    pub(super) fn pow10(exponent: u32) -> Self {
        let one = Self {
            limbs: [1, 0, 0, 0],
        };
        (0..exponent).fold(one, |value, _| {
            value
                .checked_mul_add(10, 0)
                .expect("a decimal power of at most 77 digits fits 256 bits")
        })
    }

    /// `self * multiplier + addend`, or `None` beyond 256 bits.
    pub(super) fn checked_mul_add(
        self,
        multiplier: u64,
        addend: u64,
    ) -> Option<Self> {
        let mut carry = u128::from(addend);
        let mut limbs = self.limbs;
        for limb in &mut limbs {
            let value = u128::from(*limb) * u128::from(multiplier) + carry;
            *limb = value as u64;
            carry = value >> u64::BITS;
        }
        (carry == 0).then_some(Self { limbs })
    }

    /// Signed big-endian two's-complement bytes of `±self`. The caller keeps
    /// the magnitude below 2^255.
    pub(super) fn to_signed_be_bytes(self, negative: bool) -> [u8; I256_BYTES] {
        let mut bytes = [0u8; I256_BYTES];
        for (index, limb) in self.limbs.iter().enumerate() {
            let end = I256_BYTES - index * std::mem::size_of::<u64>();
            bytes[end - std::mem::size_of::<u64>()..end]
                .copy_from_slice(&limb.to_be_bytes());
        }
        if negative {
            for byte in &mut bytes {
                *byte = !*byte;
            }
            for byte in bytes.iter_mut().rev() {
                let (next, carry) = byte.overflowing_add(1);
                *byte = next;
                if !carry {
                    break;
                }
            }
        }
        bytes
    }

    pub(super) fn is_zero(self) -> bool {
        self.limbs.iter().all(|limb| *limb == 0)
    }

    pub(super) fn decimal_digit_count(self) -> i32 {
        let mut value = self;
        let mut groups = 0;
        let mut highest = 0;
        while !value.is_zero() {
            highest = value.div_rem(NUMERIC_BASE);
            groups += 1;
        }
        (groups - 1) * DECIMAL_DIGITS_PER_GROUP + Self::group_digit_count(highest)
    }

    const fn group_digit_count(group: u64) -> i32 {
        if group >= 1_000 {
            4
        } else if group >= 100 {
            3
        } else if group >= 10 {
            2
        } else {
            1
        }
    }

    fn div_rem(&mut self, divisor: u64) -> u64 {
        let mut remainder = 0u128;
        for limb in self.limbs.iter_mut().rev() {
            let value = (remainder << u64::BITS) | u128::from(*limb);
            *limb = u64::try_from(value / u128::from(divisor))
                .expect("u256 long division produces one u64 quotient limb");
            remainder = value % u128::from(divisor);
        }
        u64::try_from(remainder)
            .expect("u256 long-division remainder is smaller than its u64 divisor")
    }
}
