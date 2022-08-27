use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

pub(crate) fn rand_alphanumeric(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
